package step.streaming.websocket.client.upload;

import jakarta.websocket.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.StreamingResourceReference;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.data.CheckpointingOutputStream;
import step.streaming.data.MD5CalculatingOutputStream;
import step.streaming.websocket.protocol.upload.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class WebsocketUploadClient {
    public static final long DEFAULT_REPORT_INTERVAL_MS = 1000;

    private static final Logger logger = LoggerFactory.getLogger(WebsocketUploadClient.class);

    private enum State {
        EXPECTING_REFERENCE,
        UPLOADING,
        EXPECTING_FINALIZATION,
        FINALIZED,
        CLOSED
    }

    private final WebsocketUploadClient self = this;
    private final Session session;
    private final long reportIntervalMs;
    private final WebsocketUpload upload;
    // this will be populated in response to the upload message
    private final CompletableFuture<StreamingResourceReference> referenceFuture = new CompletableFuture<>();
    // populated in response to finalize message
    private final CompletableFuture<UploadFinishedMessage> uploadFinishedFuture = new CompletableFuture<>();
    private State state;

    @Override
    public String toString() {
        return String.format("{session=%s}", session.getId());
    }

    public WebsocketUploadClient(URI endpointUri, WebsocketUpload upload) throws IOException {
        this(endpointUri, upload, ContainerProvider.getWebSocketContainer(), DEFAULT_REPORT_INTERVAL_MS);
    }

    public WebsocketUploadClient(URI endpointUri, WebsocketUpload upload, WebSocketContainer container, long reportIntervalMs) throws IOException {
        UploadProtocolMessage.initialize();
        this.upload = Objects.requireNonNull(upload);
        upload.onClose(this::onUploadClosed);
        this.reportIntervalMs = reportIntervalMs;
        session = connect(endpointUri, container);
        logger.debug("{} Connected to endpoint {}", this, endpointUri);
        // immediately send the upload inititation message; this might (theoretically) throw an exception
        sendUploadRequestAndAwaitReply();
    }

    private Session connect(URI websocketUri, WebSocketContainer container) throws IOException {
        try {
            return container.connectToServer(new Remote(), ClientEndpointConfig.Builder.create().build(), websocketUri);
        } catch (DeploymentException e) {
            throw new IOException(e);
        }
    }

    private void onMessage(UploadServerMessage message) {
        logger.debug("onMessage: {}", message);
        if (state == State.EXPECTING_REFERENCE && message instanceof ReadyForUploadMessage) {
            referenceFuture.complete(((ReadyForUploadMessage) message).reference);
        } else if (state == State.EXPECTING_FINALIZATION && message instanceof UploadFinishedMessage) {
            uploadFinishedFuture.complete((UploadFinishedMessage) message);
        } else {
            throw new IllegalArgumentException("Unexpected message type: " + message.getClass().getSimpleName() + " in state " + state);
        }
    }

    private void sendUploadRequestAndAwaitReply() throws IOException {
        RequestUploadStartMessage request = new RequestUploadStartMessage(upload.getMetadata());
        state = State.EXPECTING_REFERENCE;
        session.getBasicRemote().sendText(request.toString());
        try {
            StreamingResourceReference reference = referenceFuture.get(5, TimeUnit.SECONDS);
            upload.setUploadReference(reference);
            upload.setCurrentUploadStatus(new StreamingResourceStatus(StreamingResourceTransferStatus.INITIATED, 0L, null));
            state = State.UPLOADING;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    private void closeCloseableOnException(AutoCloseable closeable, Exception outer) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (Exception inner) {
            outer.addSuppressed(inner);
        }
    }

    private void closeSessionAbnormally(String message, Exception optionalExceptionContext) {
        try {
            session.close(new CloseReason(CloseReason.CloseCodes.UNEXPECTED_CONDITION, message));
        } catch (Exception inner) {
            if (optionalExceptionContext != null) {
                optionalExceptionContext.addSuppressed(inner);
            }
        }
    }


    public void performUpload(InputStream inputStream) {
        MD5CalculatingOutputStream outputStream = null;
        try {
            if (state != State.UPLOADING) {
                throw new IllegalStateException("Requested data upload, but state is " + state);
            }
            outputStream = new MD5CalculatingOutputStream(new CheckpointingOutputStream(session.getBasicRemote().getSendStream(), reportIntervalMs, this::updateInProgressTransferSize));
            long bytesSent = inputStream.transferTo(outputStream);
            // We explicitly close the input and output to also catch any potential exceptions there. This is why we don't use try-with-resources.
            inputStream.close();
            outputStream.close();
            logger.debug("{} Data sent: {} bytes", this, bytesSent);
            state = State.EXPECTING_FINALIZATION;
            UploadFinishedMessage finished = uploadFinishedFuture.get();
            String clientChecksum = outputStream.getChecksum();
            String serverChecksum = finished.checksum;
            if (!clientChecksum.equals(serverChecksum)) {
                throw new IOException("checksum mismatch: client reported " + clientChecksum + ", but server reported " + serverChecksum);
            }
            state = State.FINALIZED;
            CloseReason closeReason = new CloseReason(CloseReason.CloseCodes.NORMAL_CLOSURE, UploadProtocolMessage.UPLOAD_COMPLETED);
            logger.info("About to close session with reason: {}", closeReason);
            session.close(closeReason);
            // wait until upload is closed too
            upload.getFinalStatusFuture().join();
        } catch (Exception exception) {
            closeCloseableOnException(inputStream, exception);
            closeCloseableOnException(outputStream, exception);
            closeSessionAbnormally(exception.getMessage(), exception);
        }
    }

    private void updateInProgressTransferSize(long transferredBytes) {
        if (!upload.getFinalStatusFuture().isDone()) {
            upload.setCurrentUploadStatus(new StreamingResourceStatus(StreamingResourceTransferStatus.IN_PROGRESS, transferredBytes, null));
        }
    }

    private void onError(Throwable throwable) {
        logger.error("{} Unexpected error", this, throwable);
        upload.getFinalStatusFuture().completeExceptionally(throwable);
    }

    private void onClose(CloseReason closeReason) {
        if (state == State.FINALIZED) {
            // normal closure
            logger.info("{} Session closing, reason={}", this, closeReason);
            UploadFinishedMessage finishedMessage = uploadFinishedFuture.join();
            upload.getFinalStatusFuture().complete(new StreamingResourceStatus(StreamingResourceTransferStatus.COMPLETED, finishedMessage.size, finishedMessage.numberOflines));
        } else {
            logger.warn("{} Unexpected closure of session, reason={}, currently in state {}", this, closeReason, state);
            // terminate open futures if any
            IllegalStateException exception = new IllegalStateException("Client closed, reason=" + closeReason + ", but upload was not completed");
            if (!referenceFuture.isDone()) {
                referenceFuture.completeExceptionally(exception);
            }
            if (!uploadFinishedFuture.isDone()) {
                uploadFinishedFuture.completeExceptionally(exception);
            }
            if (!upload.getFinalStatusFuture().isDone()) {
                upload.getFinalStatusFuture().completeExceptionally(exception);
            }
        }
        state = State.CLOSED;
    }

    private void onUploadClosed() {
        if (state != State.CLOSED) {
            logger.warn("{} Upload closed, but client is still in state {}; Closing session abnormally", this, state);
            closeSessionAbnormally("Upload was unexpectedly closed", null);
        }
    }


    private class Remote extends Endpoint {
        @Override
        public void onOpen(Session session, EndpointConfig config) {
            // No timeout; server side takes care of keepalive messages
            session.setMaxIdleTimeout(0);
            session.addMessageHandler(String.class, message -> self.onMessage(UploadServerMessage.fromString(message)));
        }

        @Override
        public void onClose(Session session, CloseReason closeReason) {
            self.onClose(closeReason);
        }

        @Override
        public void onError(Session session, Throwable throwable) {
            self.onError(throwable);
        }
    }

}
