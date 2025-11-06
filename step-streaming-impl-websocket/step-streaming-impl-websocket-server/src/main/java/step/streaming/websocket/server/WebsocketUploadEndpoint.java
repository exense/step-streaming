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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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

                uploadPipeline = new UploadPipeline(resourceId, uploadsPool, manager);
                uploadAcknowledgedMessage = uploadPipeline.startConsumer();

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

    // ---------------------- Internal helpers ----------------------
    // Code / comments below partly AI-assisted

    /**
     * UploadPipeline — a cooperative, per-upload drain pipeline.
     *
     * <p>This component asynchronously drains incoming byte chunks for a single upload (originating from Jetty)
     * using an {@link ExecutorService} shared between uploads.
     *
     * <ul>
     *   <li>Uses the shared {@code uploadsPool}; no dedicated thread per upload.</li>
     *   <li>Implements bounded backpressure via a {@link Semaphore} ({@code QUEUE_SLOTS}).</li>
     *   <li>Ensures that only one drain task (per upload) runs at a time via a work-in-progress counter.</li>
     *   <li>Batches chunks up to a fixed byte limit per pass for fairness across uploads.</li>
     *   <li>Writes data through {@link StreamingResourceManager#writeChunk(String, InputStream, boolean)}.</li>
     *   <li>Completes the upload with an {@link UploadAcknowledgedMessage} once fully drained.</li>
     *   <li>Eliminates the lost-signal/deadlock issue of the previous design (SED-4327).</li>
     * </ul>
     *
     * <p>Thread-safe and non-blocking except for producer-side backpressure.
     */
    private static final class UploadPipeline {
        private static final int QUEUE_SLOTS = 128;             // How many (Jetty, incoming) chunks for a single upload can we enqueue?
        private static final int MAX_BATCH_BYTES = 256 * 1024;   // max bytes per write (for fairness between multiple uploads)

        private final String resourceId;
        private final ExecutorService uploadsPool;
        private final StreamingResourceManager manager;

        // Capacity/backpressure & non-blocking queue
        private final Semaphore queueSlots = new Semaphore(QUEUE_SLOTS);
        private final ConcurrentLinkedQueue<byte[]> bytesQueue = new ConcurrentLinkedQueue<>();

        // Work-in-progress counter; >0 means a drain task is scheduled/running
        private final AtomicInteger activeWork = new AtomicInteger(0);

        private volatile boolean closed;

        private final MessageDigest md5;
        private long bytesWritten;
        private boolean finalSent;
        private CompletableFuture<UploadAcknowledgedMessage> ackFuture;

        UploadPipeline(String resourceId, ExecutorService uploadsPool, StreamingResourceManager manager) {
            this.resourceId = resourceId;
            this.uploadsPool = uploadsPool;
            this.manager = manager;
            try {
                this.md5 = java.security.MessageDigest.getInstance("MD5");
            } catch (java.security.NoSuchAlgorithmException e) {
                // won't happen
                throw new IllegalStateException("MD5 not available", e);
            }
        }

        /**
         * Enqueue a chunk; bounded by capacity (backpressure).
         */
        void put(byte[] chunk) throws InterruptedException {
            queueSlots.acquire();
            bytesQueue.offer(chunk);
            signalWork();
        }

        /**
         * No more input; trigger finalization when queue drains.
         */
        void closeInput() {
            closed = true;
            signalWork();
        }

        /**
         * Start; returns future that completes once all bytes are written and ack is ready.
         */
        CompletableFuture<UploadAcknowledgedMessage> startConsumer() {
            this.ackFuture = new CompletableFuture<>();
            signalWork();
            return ackFuture;
        }

        /**
         * Ensure a single drain task is scheduled; coalesces multiple signals.
         */
        private void signalWork() {
            // Invariant: activeWork == 0 means "nothing is currently scheduled or working"; in addition,
            // only exactly one worker will ever enter and schedule processWork at the same time - this is guaranteed
            // by the semaphore which can be returned to 0 only *within* that processWork() call before it exits.
            if (activeWork.getAndIncrement() == 0) {
                try {
                    uploadsPool.execute(this::processWork);
                } catch (RuntimeException ex) {
                    // Roll back the signal in case something goes wrong, to keep the invariant in place
                    activeWork.decrementAndGet();
                    throw ex;
                }
            }
        }

        /**
         * Drain the queue in batches until no more signals remain (activeWork goes to 0).
         */
        private void processWork() {
            try {
                int signalsToConsume = 1;

                // Run while work is signalled and the upload hasn't completed
                while (signalsToConsume > 0 && !ackFuture.isDone()) {
                    int batchByteCount = 0;
                    ByteArrayOutputStream batchBytes = new ByteArrayOutputStream();

                    // Batch up to MAX_BATCH_BYTES for fairness; if closed (all data
                    // has arrived), we must keep processing until everything is drained,
                    // as there will be no more signals "kicking" new work iterations
                    while (closed || batchByteCount < MAX_BATCH_BYTES) {
                        byte[] chunk = bytesQueue.poll();
                        if (chunk == null) {
                            break;
                        }
                        md5.update(chunk);
                        batchBytes.write(chunk, 0, chunk.length);
                        batchByteCount += chunk.length;
                        // Release capacity for each processed chunk
                        queueSlots.release();
                    }

                    if (batchByteCount > 0) {
                        boolean isLastBatch = closed && bytesQueue.isEmpty();
                        try (InputStream combined = new ByteArrayInputStream(batchBytes.toByteArray())) {
                            manager.writeChunk(resourceId, combined, isLastBatch);
                        }
                        bytesWritten += batchByteCount;
                        if (isLastBatch) {
                            finalSent = true;
                        }
                    }
                    if (closed) {
                        // Finalize once: empty final write if not sent yet (e.g. 0-byte uploads), compute checksum, complete ack
                        if (!finalSent) {
                            try (InputStream empty = new ByteArrayInputStream(new byte[0])) {
                                manager.writeChunk(resourceId, empty, true);
                            }
                            finalSent = true;
                        }
                        String checksum = toHex(md5.digest());
                        StreamingResourceStatus status = manager.getStatus(resourceId);
                        Long lines = status.getNumberOfLines();
                        if (!ackFuture.isDone()) {
                            // Completing the future will cause the while-loop to exit on its next check
                            ackFuture.complete(new UploadAcknowledgedMessage(bytesWritten, lines, checksum));
                        }
                    }
                    // This will keep the outer loop active as long as at least one
                    // other signal for "hey, there's work" was present (but consume all of them at once)
                    signalsToConsume = activeWork.addAndGet(-signalsToConsume);
                }
            } catch (Throwable t) {
                if (!ackFuture.isDone()) {
                    ackFuture.completeExceptionally(t);
                }
                // Abnormal termination — explicitly restore invariant; this is mostly for consistency, as
                // after the result is completed with an exception, we're not expecting any more work anyway.
                activeWork.set(0);
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
