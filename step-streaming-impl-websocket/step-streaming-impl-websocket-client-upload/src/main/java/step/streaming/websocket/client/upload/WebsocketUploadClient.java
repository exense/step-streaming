package step.streaming.websocket.client.upload;

import jakarta.websocket.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.QuotaExceededException;
import step.streaming.common.StreamingResourceReference;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.data.CheckpointingOutputStream;
import step.streaming.data.MD5CalculatingOutputStream;
import step.streaming.websocket.HalfCloseCompatibleEndpoint;
import step.streaming.websocket.protocol.upload.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.channels.ClosedChannelException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class WebsocketUploadClient {
    public static final long DEFAULT_LOCAL_STATUS_UPDATE_INTERVAL_MS = 1000;

    private static final Logger logger = LoggerFactory.getLogger(WebsocketUploadClient.class);

    private enum State {
        EXPECTING_REFERENCE,
        UPLOADING,
        EXPECTING_ACKNOWLEDGE,
        FINALIZED,
        CLOSED
    }

    private final WebsocketUploadClient self = this;
    private final Session jettySession;
    // how often does the local status (i.e. number of bytes transferred) get updated?
    private final long localStatusUpdateIntervalMs;
    private final WebsocketUploadSession uploadSession;
    // this will be populated in response to the upload message
    private final CompletableFuture<StreamingResourceReference> referenceFuture = new CompletableFuture<>();
    // populated in response to finalize message
    private final CompletableFuture<UploadAcknowledgedMessage> uploadAcknowledgedFuture = new CompletableFuture<>();
    private State state;
    private Remote endpoint;
    // captured, and used in exception handling for clearer messages
    private volatile CloseReason closeReason;
    // this is just a (very generous) timeout limit for awaiting responses/events; might be configurable in the future
    private final long timeoutSeconds = 60;

    @Override
    public String toString() {
        return String.format("{session=%s}", jettySession.getId());
    }

    public WebsocketUploadClient(URI endpointUri, WebsocketUploadSession uploadSession) throws IOException {
        this(endpointUri, uploadSession, ContainerProvider.getWebSocketContainer(), DEFAULT_LOCAL_STATUS_UPDATE_INTERVAL_MS);
    }

    public WebsocketUploadClient(URI endpointUri, WebsocketUploadSession uploadSession, WebSocketContainer container, long localStatusUpdateIntervalMs) throws IOException {
        UploadProtocolMessage.initialize();
        this.uploadSession = Objects.requireNonNull(uploadSession);
        uploadSession.onClose(this::onUploadSessionClosed);
        this.localStatusUpdateIntervalMs = localStatusUpdateIntervalMs;
        jettySession = connect(endpointUri, container);
        logger.debug("{} Connected to endpoint {}", this, endpointUri);
        // immediately send the upload inititation message; this might (theoretically) throw an exception
        sendUploadRequestAndAwaitReply();
    }

    private Session connect(URI websocketUri, WebSocketContainer container) throws IOException {
        try {
            endpoint = new Remote();
            return container.connectToServer(endpoint, ClientEndpointConfig.Builder.create().build(), websocketUri);
        } catch (DeploymentException e) {
            throw new IOException(e);
        }
    }

    private void onMessage(UploadServerMessage message) {
        logger.debug("onMessage: {}", message);
        if (state == State.EXPECTING_REFERENCE && message instanceof ReadyForUploadMessage) {
            referenceFuture.complete(((ReadyForUploadMessage) message).reference);
        } else if (state == State.EXPECTING_ACKNOWLEDGE && message instanceof UploadAcknowledgedMessage) {
            uploadAcknowledgedFuture.complete((UploadAcknowledgedMessage) message);
        } else {
            throw new IllegalArgumentException("Unexpected message type: " + message.getClass().getSimpleName() + " in state " + state);
        }
    }

    // Sends text over the websocket and emits more understandable exceptions in case the session was closed
    // (e.g. by the server refusing to handle an upload because of quota checks)
    private void sendText(String text) throws IOException {
        try {
            jettySession.getBasicRemote().sendText(text);
        } catch (IOException e) {
            if (e instanceof ClosedChannelException || e.getCause() instanceof ClosedChannelException) {
                if (closeReason != null) {
                    throw new IOException(
                            "WebSocket closed by server: " + closeReason, e
                    );
                } else {
                    throw new IOException("WebSocket channel closed unexpectedly", e);
                }
            }
            throw e; // some other IOException
        }
    }

    private void sendUploadRequestAndAwaitReply() throws IOException {
        StartUploadMessage request = new StartUploadMessage(uploadSession.getMetadata());
        logger.info("{} Starting upload, metadata={}", this, request.metadata);
        state = State.EXPECTING_REFERENCE;
        sendText(request.toString());
        try {
            // response is usually immediate, but allow for a little more time (though not unlimited)
            StreamingResourceReference reference = referenceFuture.get(timeoutSeconds, TimeUnit.SECONDS);
            uploadSession.setReference(reference);
            uploadSession.setCurrentStatus(new StreamingResourceStatus(StreamingResourceTransferStatus.INITIATED, 0L, null));
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

    private void closeSessionAbnormally(String message, Exception optionalOuterExceptionContext) {
        try {
            endpoint.closeSession(jettySession, new CloseReason(CloseReason.CloseCodes.UNEXPECTED_CONDITION, message));
        } catch (Exception inner) {
            if (optionalOuterExceptionContext != null) {
                optionalOuterExceptionContext.addSuppressed(inner);
            }
        }
    }


    public void performUpload(InputStream inputStream) {
        MD5CalculatingOutputStream outputStream = null;
        try {
            if (state != State.UPLOADING) {
                throw new IllegalStateException("Requested data upload, but state is " + state);
            }
            outputStream = new MD5CalculatingOutputStream(new CheckpointingOutputStream(jettySession.getBasicRemote().getSendStream(), localStatusUpdateIntervalMs, this::updateInProgressTransferSize));
            long bytesSent = inputStream.transferTo(outputStream);
            // We explicitly close the input and output to also catch any potential exceptions there. This is why we don't use try-with-resources.
            inputStream.close();
            outputStream.close();
            String clientChecksum = outputStream.getChecksum();
            logger.debug("{} Data sent: {} bytes, checksum={}", this, bytesSent, clientChecksum);
            sendText(new FinishUploadMessage(clientChecksum).toString());
            state = State.EXPECTING_ACKNOWLEDGE;
            UploadAcknowledgedMessage acknowledgedMessage = uploadAcknowledgedFuture.get(timeoutSeconds, TimeUnit.SECONDS);
            String serverChecksum = acknowledgedMessage.checksum;
            if (!clientChecksum.equals(serverChecksum)) {
                // This is actually redundant and should never be reached - in the current implementation, the server
                // detects any mismatch and closes the session abnormally instead of sending the ACK message
                throw new IOException("checksum mismatch: client reported " + clientChecksum + ", but server reported " + serverChecksum);
            }
            state = State.FINALIZED;
            closeSessionNormally();

            // wait until the uploadSession final status is also set (should be a side effect of closing the session)
            uploadSession.getFinalStatusFuture().get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (Exception exception) {
            closeCloseableOnException(inputStream, exception);
            closeCloseableOnException(outputStream, exception);
            logger.error("Upload failed:", exception);
            closeSessionAbnormally(exception.getMessage(), exception);
        }
    }

    private void updateInProgressTransferSize(long transferredBytes) {
        if (!uploadSession.getFinalStatusFuture().isDone()) {
            uploadSession.setCurrentStatus(new StreamingResourceStatus(StreamingResourceTransferStatus.IN_PROGRESS, transferredBytes, null));
        }
    }

    private void closeSessionNormally() throws IOException {
        CloseReason closeReason = new CloseReason(CloseReason.CloseCodes.NORMAL_CLOSURE, UploadProtocolMessage.CLOSEREASON_PHRASE_UPLOAD_COMPLETED);
        logger.debug("About to close session {} with reason: {}", jettySession.getId(), closeReason);
        endpoint.closeSession(jettySession, closeReason);
    }

    private void onError(Throwable throwable) {
        logger.error("{} Unexpected error", this, throwable);
        uploadSession.getFinalStatusFuture().completeExceptionally(throwable);
    }

    private void onClose(CloseReason closeReason) {
        this.closeReason = closeReason;
        if (state == State.FINALIZED) {
            // normal closure
            logger.debug("{} Session closing, reason={}", this, closeReason);
            // uploadFinishedFuture is guaranteed to have completed before FINALIZED state is set
            UploadAcknowledgedMessage finishedMessage = uploadAcknowledgedFuture.join();
            uploadSession.getFinalStatusFuture().complete(new StreamingResourceStatus(StreamingResourceTransferStatus.COMPLETED, finishedMessage.size, finishedMessage.numberOflines));
        } else {
            logger.warn("{} Unexpected closure of session, reason={}, currently in state {}", this, closeReason, state);
            // terminate open futures if any
            Exception exception = new IOException("Websocket session closed, reason=" + closeReason + ", but upload was not completed");
            // special handling of particular exception
            if (closeReason.getReasonPhrase().startsWith("QuotaExceededException: ")) {
                exception = new QuotaExceededException(closeReason.getReasonPhrase().substring(24));
            }
            if (!referenceFuture.isDone()) {
                referenceFuture.completeExceptionally(exception);
            }
            if (!uploadAcknowledgedFuture.isDone()) {
                uploadAcknowledgedFuture.completeExceptionally(exception);
            }
            if (!uploadSession.getFinalStatusFuture().isDone()) {
                uploadSession.getFinalStatusFuture().completeExceptionally(exception);
            }
        }
        state = State.CLOSED;
    }

    private void onUploadSessionClosed(String message) {
        // if client was already closed by framework, do nothing; otherwise close client session abnormally
        if (state != State.CLOSED) {
            logger.warn("{} Upload session was closed, but client is still in state {}; Closing session abnormally", this, state);
            closeSessionAbnormally(message != null ? message : "Upload session was unexpectedly closed", null);
        }
    }


    private class Remote extends HalfCloseCompatibleEndpoint {
        @Override
        public void onOpen(Session session, EndpointConfig config) {
            // No timeout; server side takes care of keepalive messages
            session.setMaxIdleTimeout(0);
            session.addMessageHandler(String.class, message -> self.onMessage(UploadServerMessage.fromString(message)));
        }

        @Override
        public void onSessionClose(Session session, CloseReason closeReason) {
            self.onClose(closeReason);
        }

        @Override
        public void onSessionError(Session session, Throwable throwable) {
            self.onError(throwable);
        }
    }

}
