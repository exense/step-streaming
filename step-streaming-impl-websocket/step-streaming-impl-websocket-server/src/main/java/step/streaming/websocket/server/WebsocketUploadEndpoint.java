package step.streaming.websocket.server;

import jakarta.websocket.CloseReason;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.MessageHandler;
import jakarta.websocket.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.*;
import step.streaming.server.StreamingResourceManager;
import step.streaming.websocket.CloseReasonUtil;
import step.streaming.websocket.HalfCloseCompatibleEndpoint;
import step.streaming.websocket.protocol.upload.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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
        logger.warn("Closing Websocket Session with error: {}", exception.getMessage(), exception);
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
                throw new IllegalStateException("uploadAcknowledgedMessage not initialized");
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
                var finalStatus = manager.markCompleted(resourceId);
                UploadAcknowledgedMessage finalAck = ack;
                if (finalStatus != null) {
                    // numberOfLines may have changed during the final completion
                    finalAck = new UploadAcknowledgedMessage(ack.size, finalStatus.getNumberOfLines(), ack.checksum);
                }
                state = State.FINISHED;
                logger.info("Upload complete, sending acknowledge message: {}", finalAck);
                session.getAsyncRemote().sendText(finalAck.toString(), result -> {
                    if (!result.isOK()) {
                        logger.warn("{}: failed to send upload acknowledge", resourceId, result.getException());
                    }
                });
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
                logger.debug("Session closed: {}, resourceId={}, reason={}",
                        session.getId(), resourceId, closeReason);
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
     * Cooperative, per-upload serial runner.
     * - Uses the shared uploadsPool (no dedicated thread per upload).
     * - Processes a small batch per hop, then yields.
     * - Calls manager.writeChunk(resourceId, in, isFinal) with isFinal=true exactly once.
     * - Handles 0-byte uploads via a final empty write.
     */
    private static final class UploadPipeline {
        private static final int PER_UPLOAD_QUEUE_CAPACITY = 128;
        private static final int MAX_BATCH_CHUNKS = 32; // fairness: how many chunks per hop
        private static final int MAX_BATCH_SIZE = 128 * 1024; // maximum bytes processed in one batch

        private final String resourceId;
        private final ExecutorService uploadsPool;
        private final BlockingQueue<byte[]> queue = new ArrayBlockingQueue<>(PER_UPLOAD_QUEUE_CAPACITY);
        private final AtomicBoolean running = new AtomicBoolean(false);

        private volatile boolean closed;

        // set in startConsumer(...)
        private StreamingResourceManager manager;
        private MessageDigest md;
        private long bytesWritten;
        private boolean finalSent;
        private CompletableFuture<UploadAcknowledgedMessage> ackFuture;

        UploadPipeline(String resourceId, ExecutorService uploadsPool) {
            this.resourceId = resourceId;
            this.uploadsPool = uploadsPool;
        }

        /** Enqueue a chunk; bounded queue provides backpressure to the Jetty thread. */
        void put(byte[] chunk) throws InterruptedException {
            // try non-blocking first; if full, block this one connection briefly
            if (!queue.offer(chunk)) {
                queue.put(chunk);
            }
            schedule();
        }

        /** Signal that no more input will arrive. */
        void closeInput() {
            closed = true;
            schedule();
        }

        /** Start draining; returns a future that completes once all bytes are written and ack is ready. */
        CompletableFuture<UploadAcknowledgedMessage> startConsumer(StreamingResourceManager manager) {
            this.manager = manager;
            try {
                this.md = java.security.MessageDigest.getInstance("MD5");
            } catch (java.security.NoSuchAlgorithmException e) {
                throw new IllegalStateException("MD5 not available", e);
            }
            this.ackFuture = new CompletableFuture<>();
            schedule();
            return ackFuture;
        }

        /** Ensure exactly one runner is active; if not, enqueue one hop. */
        private void schedule() {
            if (running.compareAndSet(false, true)) {
                uploadsPool.execute(this::runOnce);
            }
        }

        /** Process a small batch, then either finalize, reschedule, or yield idle. */
        private void runOnce() {
            if (ackFuture.isCompletedExceptionally()) {
                logger.debug("Upload {} already failed, skipping work", resourceId);
                return;
            }
            boolean moreWork = false;
            try {
                int processed = 0;
                int totalBytes = 0;
                ByteArrayOutputStream batch = new ByteArrayOutputStream();

                byte[] chunk;
                while (processed < MAX_BATCH_CHUNKS && totalBytes <= MAX_BATCH_SIZE && (chunk = queue.poll()) != null) {
                    md.update(chunk);
                    batch.write(chunk);
                    totalBytes += chunk.length;
                    processed++;
                }

                if (processed > 0) {
                    // After popping this batch, if input is closed and nothing remains, this batch is the final write.
                    boolean willBeLastBatch = closed && queue.isEmpty();

                    try (InputStream combined = new ByteArrayInputStream(batch.toByteArray())) {
                        manager.writeChunk(resourceId, combined, willBeLastBatch);
                    }

                    bytesWritten += totalBytes;
                    if (willBeLastBatch) finalSent = true;
                }

                if (!queue.isEmpty()) {
                    // more chunks ready right now — keep going soon
                    moreWork = true;
                } else if (closed) {
                    // input closed and queue empty => finalize if needed, then complete ack
                    if (!finalSent) {
                        try (InputStream empty = new ByteArrayInputStream(new byte[0])) {
                            manager.writeChunk(resourceId, empty, true);
                        }
                        finalSent = true;
                    }
                    String checksum = toHex(md.digest());
                    StreamingResourceStatus status = manager.getStatus(resourceId);
                    Long lines = status.getNumberOfLines();
                    ackFuture.complete(new UploadAcknowledgedMessage(bytesWritten, lines, checksum));
                }
            } catch (Throwable t) {
                ackFuture.completeExceptionally(t);
            } finally {
                // yield: drop the running flag, and if work showed up in the meantime, re-arm
                running.set(false);
                if ((moreWork || !queue.isEmpty()) && running.compareAndSet(false, true)) {
                    uploadsPool.execute(this::runOnce);
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
}
