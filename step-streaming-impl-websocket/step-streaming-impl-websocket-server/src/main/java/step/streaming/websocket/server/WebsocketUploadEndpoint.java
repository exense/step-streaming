package step.streaming.websocket.server;

import jakarta.websocket.CloseReason;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.MessageHandler;
import jakarta.websocket.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.*;
import step.streaming.data.MD5CalculatingInputStream;
import step.streaming.server.StreamingResourceManager;
import step.streaming.websocket.CloseReasonUtil;
import step.streaming.websocket.HalfCloseCompatibleEndpoint;
import step.streaming.websocket.protocol.upload.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

public class WebsocketUploadEndpoint extends HalfCloseCompatibleEndpoint {
    public static final String DEFAULT_ENDPOINT_URL = "/ws/streaming/upload";
    // how long we wait on abnormal close for the asynchronous upload consumer to drain already queued bytes
    private static final long ABNORMAL_CLOSE_DRAIN_TIMEOUT_MS = 200;

    private enum State {
        EXPECTING_METADATA,
        UPLOADING,
        EXPECTING_FINISHEDMESSAGE,
        FINISHED,
    }

    private static final Logger logger = LoggerFactory.getLogger(WebsocketUploadEndpoint.class);

    protected final StreamingResourceManager manager;
    private final WebsocketServerEndpointSessionsHandler sessionsHandler;

    protected Session session;
    private State state;
    private CompletableFuture<UploadAcknowledgedMessage> uploadAcknowledgedMessage;

    protected String resourceId;
    protected String uploadContextId;

    // Per-session pipeline (created on first data frame)
    private UploadPipeline uploadPipeline;
    private final ExecutorService uploadsPool;

    public WebsocketUploadEndpoint(StreamingResourceManager manager, WebsocketServerEndpointSessionsHandler sessionsHandler) {
        UploadProtocolMessage.initialize();
        this.manager = manager;
        this.sessionsHandler = sessionsHandler;
        this.uploadsPool = manager.getUploadsThreadPool();
    }

    @Override
    public void onOpen(Session session, EndpointConfig config) {
        this.session = session;
        session.getRequestParameterMap()
                .getOrDefault(StreamingResourceUploadContext.PARAMETER_NAME, List.of())
                .stream().findFirst().ifPresent(ctx -> uploadContextId = ctx);
        if (manager.isUploadContextRequired() && uploadContextId == null) {
            closeSession(session, CloseReasonUtil.makeSafeCloseReason(
                    CloseReason.CloseCodes.VIOLATED_POLICY,
                    "Missing parameter " + StreamingResourceUploadContext.PARAMETER_NAME));
            return;
        }
        Optional.ofNullable(sessionsHandler).ifPresent(handler -> handler.register(session));
        state = State.EXPECTING_METADATA;
        session.addMessageHandler(String.class, this::onMessage);
        // conceptually the same as reading the data directly from a stream, but better for
        // performance as it does not block Jetty threads.
        session.addMessageHandler(
                ByteBuffer.class,
                // DO NOT REPLACE WITH A LAMBDA -- this will trip up Jetty at runtime
                new MessageHandler.Partial<ByteBuffer>() {
                    @Override
                    public void onMessage(ByteBuffer data, boolean last) {
                        onDataPartial(data, last);
                    }
                }
        );

        logger.debug("Session opened: {}", session.getId());
    }

    private void closeSession(Throwable exception) {
        // because of some limitations, the QuotaExceededException might be wrapped inside an IOException
        if (exception instanceof IOException && exception.getCause() instanceof QuotaExceededException) {
            exception = exception.getCause();
        }
        if (exception instanceof QuotaExceededException) {
            // this one is handled specially by the client
            closeSession(session, CloseReasonUtil.makeSafeCloseReason(
                    CloseReason.CloseCodes.VIOLATED_POLICY,
                    "QuotaExceededException: " + exception.getMessage()));
        } else {
            closeSession(session, CloseReasonUtil.makeSafeCloseReason(
                    CloseReason.CloseCodes.UNEXPECTED_CONDITION,
                    exception.getMessage()));
        }
    }

    private void onMessage(String messageString) {
        if (logger.isTraceEnabled()) {
            logger.trace("Message received: {}", messageString);
        }
        UploadClientMessage clientMessage = UploadClientMessage.fromString(messageString);
        if (state == State.EXPECTING_METADATA && clientMessage instanceof StartUploadMessage) {
            StreamingResourceMetadata metadata = ((StartUploadMessage) clientMessage).metadata;
            try {
                resourceId = manager.registerNewResource(metadata, uploadContextId);
                StreamingResourceReference reference = manager.getReferenceFor(resourceId);
                logger.info("{}: Starting streaming upload, metadata={}", resourceId, metadata);

                uploadPipeline = new UploadPipeline(resourceId, uploadsPool);
                uploadAcknowledgedMessage = uploadPipeline.startConsumer(manager);

                state = State.UPLOADING;

                ReadyForUploadMessage reply = new ReadyForUploadMessage(reference);
                session.getAsyncRemote().sendText(reply.toString());
            } catch (IOException | QuotaExceededException e) {
                closeSession(e);
            }
        } else if (state == State.EXPECTING_FINISHEDMESSAGE && clientMessage instanceof FinishUploadMessage) {
            FinishUploadMessage finishMessage = (FinishUploadMessage) clientMessage;
            if (uploadAcknowledgedMessage == null) {
                throw new IllegalStateException("ackFuture not initialized");
            }
            // Wait asynchronously for the consumer to finish, then validate and respond.
            uploadAcknowledgedMessage.whenComplete((ack, ex) -> {
                if (ex != null) {
                    closeSession(ex);
                    return;
                }
                if (!ack.checksum.equals(finishMessage.checksum)) {
                    closeSession(new RuntimeException(
                            String.format("Checksum mismatch after upload! Client checksum=%s, server checksum=%s",
                                    finishMessage.checksum, ack.checksum)));
                    return;
                }
                // send ack now (response to FinishUploadMessage, just like before)
                session.getAsyncRemote().sendText(ack.toString(), result -> {
                    if (!result.isOK()) {
                        logger.warn("{}: failed to send ack", resourceId, result.getException());
                    }
                });
                manager.markCompleted(resourceId);
                state = State.FINISHED;
            });
        } else {
            throw new IllegalArgumentException("Unsupported message in state " + state + ": " + messageString);
        }
    }

    /**
     * Non-blocking-ish data path: copy ByteBuffer slices into a small queue
     * and return quickly; the consumer on UPLOAD_POOL will read them as an InputStream
     * and call manager.writeChunk(...) just like before.
     */
    private void onDataPartial(ByteBuffer data, boolean last) {
        if (state != State.UPLOADING) {
            throw new IllegalStateException("Unexpected data received in state " + state);
        }

        try {
            int remaining = data.remaining();
            if (remaining > 0) {
                byte[] copy = new byte[remaining];
                data.get(copy);
                // Backpressure: bounded queue; put blocks this one connection briefly if the consumer lags
                uploadPipeline.put(copy);
            } else {
                // explicitly consume buffer, even if empty
                data.get(new byte[0]);
            }

            if (last) {
                // binary message is complete. Close pipeline and expect finish message
                uploadPipeline.closeInput();
                state = State.EXPECTING_FINISHEDMESSAGE;
            }
        } catch (Throwable t) {
            closeSession(t);
        }
    }

    @Override
    public void onSessionClose(Session session, CloseReason closeReason) {
        Optional.ofNullable(sessionsHandler).ifPresent(handler -> handler.unregister(session));
        super.onClose(session, closeReason);
        if (resourceId != null) {
            if (closeReason.getCloseCode() == CloseReason.CloseCodes.NORMAL_CLOSURE
                    && closeReason.getReasonPhrase().equals(UploadProtocolMessage.CLOSEREASON_PHRASE_UPLOAD_COMPLETED)
                    && state == State.FINISHED) {
                logger.debug("Session closed: {}, resourceId={}, reason={}; marking as completed",
                        session.getId(), resourceId, closeReason);
                manager.markCompleted(resourceId);
            } else {
                StreamingResourceTransferStatus status = manager.getStatus(resourceId).getTransferStatus();
                logger.warn("Upload session for resource {}, state {} was closed with reason {}, resource was left with status {}; marking upload as failed.",
                        resourceId, state, closeReason, status);

                if (uploadPipeline != null) {
                    // 1) ensure no more input is expected; let consumer drain what's already queued
                    uploadPipeline.closeInput();
                    try {
                        (uploadAcknowledgedMessage != null ? uploadAcknowledgedMessage : CompletableFuture.completedFuture(null))
                                .get(ABNORMAL_CLOSE_DRAIN_TIMEOUT_MS, java.util.concurrent.TimeUnit.MILLISECONDS);
                    } catch (Exception ignoredBecauseWereFailingAnyway) {
                    }
                }
                // 3) finally mark failed (final size reflects what got drained)
                manager.markFailed(resourceId);
            }
        } else {
            logger.warn("Incomplete session (no resource ID) closed: session={}, state={}, reason={}",
                    session.getId(), state, closeReason);
        }
    }

    @Override
    public void onSessionError(Session session, Throwable throwable) {
        logger.error("Session {}, resourceId {}, state {} : unexpected error", session.getId(), resourceId, state, throwable);
        if (uploadPipeline != null) {
            uploadPipeline.closeInput();
        }
    }

    // ---------------------- Internal helpers (no external API changes) ----------------------
    // Code below is mostly AI-generated (but manually reviewed and slightly adapted)

    /**
     * Turns queued byte[] chunks into an InputStream, then calls the existing manager.writeChunk(...)
     * on a background thread. When done, completes with UploadAcknowledgedMessage (bytes, lines, md5).
     */
    /**
     * Cooperatively drains queued byte[] chunks to storage without monopolizing a thread.
     * Keeps the external API the same as before.
     */
    private static final class UploadPipeline {
        private static final int PER_UPLOAD_QUEUE_CAPACITY = 128;
        private static final int DRAIN_CHUNK_BUDGET = 16; // how many chunks to process per hop

        private final String resourceId;
        private final ExecutorService uploadsPool;
        private final BlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(PER_UPLOAD_QUEUE_CAPACITY);
        private final java.util.concurrent.atomic.AtomicBoolean draining = new java.util.concurrent.atomic.AtomicBoolean(false);

        private volatile boolean closed;

        // set when startConsumer(...) is called
        private StreamingResourceManager manager;
        private java.security.MessageDigest md; // MD5 over all bytes
        private long bytesWritten;              // total bytes written (sum of chunk sizes)
        private CompletableFuture<UploadAcknowledgedMessage> ackFuture;

        UploadPipeline(String resourceId, ExecutorService uploadsPool) {
            this.resourceId = resourceId;
            this.uploadsPool = uploadsPool;
        }

        /** Enqueue a chunk. A brief block here only affects this connection’s handler invocation. */
        void put(byte[] chunk) throws InterruptedException {
            // Try to avoid long blocking of Jetty worker threads.
            if (!queue.offer(chunk)) {
                queue.put(chunk); // fallback: bounded backpressure
            }
            scheduleDrain();
        }

        /** Signal that no more input will arrive (last frame seen). */
        void closeInput() {
            closed = true;
            scheduleDrain();
        }

        /**
         * Start draining to storage, returning a future that completes when all queued bytes
         * have been written. No WebSocket sends happen here; upper layers decide when to reply.
         */
        CompletableFuture<UploadAcknowledgedMessage> startConsumer(StreamingResourceManager manager) {
            this.manager = manager;
            try {
                this.md = java.security.MessageDigest.getInstance("MD5");
            } catch (java.security.NoSuchAlgorithmException e) {
                // MD5 is guaranteed to exist on the JDK; rethrow if somehow unavailable
                throw new IllegalStateException("MD5 not available", e);
            }
            this.ackFuture = new CompletableFuture<>();
            // Kick off (if there is work); further puts/closeInput() also call scheduleDrain()
            scheduleDrain();
            return ackFuture;
        }

        /** Ensure a drain task is enqueued if one isn't already running. */
        private void scheduleDrain() {
            if (draining.compareAndSet(false, true)) {
                uploadsPool.execute(this::drainSome);
            }
        }

        /** Process a bounded amount of work, then yield. */
        private void drainSome() {
            boolean reschedule = false;
            try {
                int budget = DRAIN_CHUNK_BUDGET;
                byte[] chunk;

                while (budget-- > 0 && (chunk = queue.poll()) != null) {
                    // Update MD5 and write this chunk
                    md.update(chunk);
                    try (java.io.InputStream in = new java.io.ByteArrayInputStream(chunk)) {
                        manager.writeChunk(resourceId, in); // append this chunk
                    }
                    bytesWritten += chunk.length;
                }

                // If more work remains, or more may still arrive, decide what to do next
                if (!queue.isEmpty()) {
                    reschedule = true; // more chunks ready now
                } else if (closed) {
                    // No queued data and input closed => finalize
                    String checksum = toHex(md.digest());
                    StreamingResourceStatus status = manager.getStatus(resourceId);
                    Long lines = status.getNumberOfLines();
                    ackFuture.complete(new UploadAcknowledgedMessage(bytesWritten, lines, checksum));
                }
                // else: queue empty but not closed yet; wait for more data (put() will reschedule)

            } catch (Throwable t) {
                // Surface the failure; upper layers decide how to close the session / mark status
                ackFuture.completeExceptionally(t);
            } finally {
                draining.set(false);
                // Race: new data may have arrived after we processed / cleared 'draining'
                if ((reschedule || (!queue.isEmpty())) && draining.compareAndSet(false, true)) {
                    uploadsPool.execute(this::drainSome);
                }
            }
        }

        private static String toHex(byte[] digest) {
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >>> 4) & 0xF, 16))
                        .append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        }
    }

    /**
     * Simple InputStream that pulls byte[] chunks from a BlockingQueue until the producer calls closeInput().
     * When closed == true and the queue is empty, it returns EOF.
     */
    private static final class ChunkQueueInputStream extends InputStream {
        private final BlockingQueue<byte[]> q;
        private final BooleanSupplier isClosed;
        private byte[] cur;
        private int off;

        ChunkQueueInputStream(BlockingQueue<byte[]> q, BooleanSupplier isClosed) {
            this.q = q;
            this.isClosed = isClosed;
        }

        @Override
        public int read() throws IOException {
            byte[] b = ensureChunk();
            if (b == null) return -1;
            return b[off++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (b == null) throw new NullPointerException();
            if (off < 0 || len < 0 || off + len > b.length) throw new IndexOutOfBoundsException();
            byte[] chunk = ensureChunk();
            if (chunk == null) return -1;
            int n = Math.min(len, chunk.length - this.off);
            System.arraycopy(chunk, this.off, b, off, n);
            this.off += n;
            return n;
        }

        private byte[] ensureChunk() throws IOException {
            try {
                while (cur == null || off >= cur.length) {
                    if (isClosed.getAsBoolean() && q.isEmpty()) return null; // EOF
                    cur = q.poll(10, TimeUnit.MILLISECONDS);
                    off = 0;
                }
                return cur;
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new EOFException("Interrupted while waiting for data");
            }
        }
    }
}
