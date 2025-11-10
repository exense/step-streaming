package step.streaming.websocket.server;

import jakarta.websocket.CloseReason;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.MessageHandler;
import jakarta.websocket.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.*;
import step.streaming.server.StreamingResourceManager;
import step.streaming.util.BatchProcessor;
import step.streaming.websocket.CloseReasonUtil;
import step.streaming.websocket.HalfCloseCompatibleEndpoint;
import step.streaming.websocket.protocol.upload.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

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
        logger.warn("Closing Websocket Session for resource {} with error: {}", resourceId, exception.getMessage(), exception);
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

                uploadPipeline = new UploadPipeline(resourceId, manager);
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
                logger.info("{} Upload complete, sending acknowledge message: {}", resourceId, finalAck);
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

    private static final class UploadPipeline {
        private final String resourceId;
        private final StreamingResourceManager manager;
        private final AtomicLong bytesWritten = new AtomicLong(0);
        private volatile boolean closed;

        private final MessageDigest md5;
        private CompletableFuture<UploadAcknowledgedMessage> ackFuture;
        private final BatchProcessor<byte[]> batchProcessor;

        private static final long MAX_QUEUE_SIZE = 256 * 1024;

        UploadPipeline(String resourceId, StreamingResourceManager manager) {
            this.resourceId = resourceId;
            this.manager = manager;
            try {
                this.md5 = java.security.MessageDigest.getInstance("MD5");
            } catch (java.security.NoSuchAlgorithmException e) {
                // won't happen
                throw new IllegalStateException("MD5 not available", e);
            }

            BatchProcessor.CountingFlushDecider<byte[]> decider = new BatchProcessor.CountingFlushDecider<>(ba -> (long) ba.length, MAX_QUEUE_SIZE);
            this.batchProcessor = new BatchProcessor<>(decider, 1000, this::processChunks, "ws-upload");
        }

        /**
         * Start; returns future that completes once all bytes are written and ack is ready.
         */
        CompletableFuture<UploadAcknowledgedMessage> startConsumer() {
            this.ackFuture = new CompletableFuture<>();
            return ackFuture;
        }

        void put(byte[] chunk) {
            int queued = batchProcessor.add(chunk);
            if (queued > 0 && logger.isDebugEnabled()) {
                logger.debug("{} Q: Queue size now {}", resourceId, queued);
            }
        }

        void closeInput() {
            if (!closed) {
                closed = true;
                // this also flushes before closing
                batchProcessor.close();
                // Rare but possible: if the batch processor had nothing to process,
                // we have to manually trigger a last round of processing
                if (!ackFuture.isDone()) {
                    processChunks(List.of());
                }
            }
        }

        private void processChunks(List<byte[]> chunks) {
            // If the result is already sent, but bytes are still asynchronously coming in,
            // ignore them. This can happen when an exception was thrown (e.g. quota exceeded).
            if (!ackFuture.isDone()) {
                try {
                    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                    long processed = 0;
                    for (byte[] chunk: chunks) {
                        md5.update(chunk);
                        bytes.write(chunk);
                        processed += chunk.length;
                    }
                    if (processed > 0 || closed) {
                        try (InputStream combined = new ByteArrayInputStream(bytes.toByteArray())) {
                            manager.writeChunk(resourceId, combined, closed);
                        }
                        bytesWritten.addAndGet(processed);
                        if (logger.isDebugEnabled()) {
                            // unless something is horribly wrong, queue size should be back to 0
                            logger.debug("{} D: Queue size now {}", resourceId, batchProcessor.getCurrentBatchSize());
                        }
                        if (closed) {
                            String checksum = toHex(md5.digest());
                            StreamingResourceStatus status = manager.getStatus(resourceId);
                            Long lines = status.getNumberOfLines();
                            if (!ackFuture.isDone()) {
                                ackFuture.complete(new UploadAcknowledgedMessage(bytesWritten.get(), lines, checksum));
                            }
                        }
                    }
                } catch (Throwable e) {
                    ackFuture.completeExceptionally(e);
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
