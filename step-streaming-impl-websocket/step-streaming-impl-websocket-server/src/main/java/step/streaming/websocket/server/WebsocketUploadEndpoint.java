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

    // Small, bounded pool for background consumers (not Jetty’s pool).
    // These are used for turning asynchronous data chunks into synchronous streams
    private static final ExecutorService UPLOAD_POOL = new ThreadPoolExecutor(
            Math.max(2, Runtime.getRuntime().availableProcessors()),
            Math.max(2, Runtime.getRuntime().availableProcessors()),
            0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(256),
            namedDaemon("websocket-upload-consumer")
    );

    private static ThreadFactory namedDaemon(String prefix) {
        return r -> {
            Thread t = new Thread(r, prefix + "-" + System.identityHashCode(r));
            t.setDaemon(true);
            return t;
        };
    }

    protected final StreamingResourceManager manager;
    private final WebsocketServerEndpointSessionsHandler sessionsHandler;

    protected Session session;
    private State state;
    private CompletableFuture<UploadAcknowledgedMessage> uploadAcknowledgedMessage;

    protected String resourceId;
    protected String uploadContextId;

    // Per-session pipeline (created on first data frame)
    private UploadPipeline uploadPipeline;

    public WebsocketUploadEndpoint(StreamingResourceManager manager, WebsocketServerEndpointSessionsHandler sessionsHandler) {
        UploadProtocolMessage.initialize();
        this.manager = manager;
        this.sessionsHandler = sessionsHandler;
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
            if (uploadPipeline == null) {
                uploadPipeline = new UploadPipeline(resourceId);
                uploadAcknowledgedMessage = uploadPipeline.startConsumer(manager);
            }

            // Copy the incoming data (spec forbids retaining the ByteBuffer after return)
            byte[] copy = new byte[data.remaining()];
            data.get(copy);

            // Backpressure: bounded queue; put blocks this one connection briefly if the consumer lags
            uploadPipeline.put(copy);

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
    private static final class UploadPipeline {
        private static final int PER_UPLOAD_QUEUE_CAPACITY = 128;

        private final String resourceId;
        private final BlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(PER_UPLOAD_QUEUE_CAPACITY);
        private volatile boolean closed;

        UploadPipeline(String resourceId) {
            this.resourceId = resourceId;
        }

        void put(byte[] chunk) throws InterruptedException {
            // A brief block here only affects this connection’s handler invocation.
            queue.put(chunk);
        }

        void closeInput() {
            closed = true;
        }

        CompletableFuture<UploadAcknowledgedMessage> startConsumer(StreamingResourceManager manager) {
            CompletableFuture<UploadAcknowledgedMessage> ackMessage = new CompletableFuture<>();
            UPLOAD_POOL.submit(() -> {
                try (InputStream queued =
                             new ChunkQueueInputStream(queue, () -> closed)) {
                    MD5CalculatingInputStream md5In = new MD5CalculatingInputStream(queued);
                    long bytesWritten = manager.writeChunk(resourceId, md5In);
                    StreamingResourceStatus status = manager.getStatus(resourceId);
                    if (status.getCurrentSize() != bytesWritten) {
                        throw new IllegalStateException("Unexpected size mismatch: bytesWritten=" + bytesWritten
                                + ", but status indicates current size=" + status.getCurrentSize());
                    }
                    String checksum = md5In.getChecksum();
                    Long lines = status.getNumberOfLines();
                    ackMessage.complete(new UploadAcknowledgedMessage(bytesWritten, lines, checksum));
                } catch (Throwable t) {
                    ackMessage.completeExceptionally(t);
                }
            });
            return ackMessage;
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
