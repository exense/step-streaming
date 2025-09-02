package step.streaming.websocket.server;

import jakarta.websocket.CloseReason;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.*;
import step.streaming.data.MD5CalculatingInputStream;
import step.streaming.server.StreamingResourceManager;
import step.streaming.websocket.CloseReasonUtil;
import step.streaming.websocket.HalfCloseCompatibleEndpoint;
import step.streaming.websocket.protocol.upload.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public class WebsocketUploadEndpoint extends HalfCloseCompatibleEndpoint {
    public static final String DEFAULT_ENDPOINT_URL = "/ws/streaming/upload";


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
    private UploadAcknowledgedMessage uploadAcknowledgedMessage; // sent to client once upload is finished

    protected String resourceId;
    protected String uploadContextId;

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
            closeSession(session, CloseReasonUtil.makeSafeCloseReason(CloseReason.CloseCodes.VIOLATED_POLICY, "Missing parameter " + StreamingResourceUploadContext.PARAMETER_NAME));
            return;
        }
        Optional.ofNullable(sessionsHandler).ifPresent(handler -> handler.register(session));
        state = State.EXPECTING_METADATA;
        session.addMessageHandler(String.class, this::onMessage);
        session.addMessageHandler(InputStream.class, this::onData);
        logger.debug("Session opened: {}", session.getId());
    }

    private void closeSession(Exception exception) {
        // because of some limitations, the QuotaExceededException might be wrapped inside an IOException
        if (exception instanceof IOException && exception.getCause() instanceof QuotaExceededException) {
            exception = (QuotaExceededException) exception.getCause();
        }
        if (exception instanceof QuotaExceededException) {
            // this one is handled specially by the client
            closeSession(session, CloseReasonUtil.makeSafeCloseReason(CloseReason.CloseCodes.VIOLATED_POLICY, "QuotaExceededException: " + exception.getMessage()));
        } else {
            closeSession(session, CloseReasonUtil.makeSafeCloseReason(CloseReason.CloseCodes.UNEXPECTED_CONDITION, exception.getMessage()));
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
                session.getBasicRemote().sendText(reply.toString());
            } catch (IOException | QuotaExceededException e) {
                closeSession(e);
            }
        } else if (state == State.EXPECTING_FINISHEDMESSAGE && clientMessage instanceof FinishUploadMessage) {
            FinishUploadMessage finishMessage = (FinishUploadMessage) clientMessage;
            if (uploadAcknowledgedMessage == null) {
                // should never happen
                throw new IllegalStateException("unexpected state: uploadAcknowledgedMessage is null");
            }
            if (!uploadAcknowledgedMessage.checksum.equals(finishMessage.checksum)) {
                throw new RuntimeException(
                        String.format("Checksum mismatch after upload! Client checksum=%s, server checksum=%s",
                                finishMessage.checksum, uploadAcknowledgedMessage.checksum));
            }
            try {
                session.getBasicRemote().sendText(uploadAcknowledgedMessage.toString());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            state = State.FINISHED;
        } else {
            throw new IllegalArgumentException("Unsupported message in state " + state + ": " + messageString);
        }
    }

    private void onData(InputStream input) {
        if (state != State.UPLOADING) {
            throw new IllegalStateException("Unexpected data received in state " + state);
        }
        try {
            MD5CalculatingInputStream md5Input = new MD5CalculatingInputStream(input);
            long bytesWritten = manager.writeChunk(resourceId, md5Input);
            StreamingResourceStatus status = manager.getStatus(resourceId);
            if (status.getCurrentSize() != bytesWritten) {
                throw new IllegalStateException("Unexpected size mismatch: bytesWritten=" + bytesWritten + ", but status indicates current size=" + status.getCurrentSize());
            }
            String checksum = md5Input.getChecksum();
            logger.info("{}: Upload finished, bytes={}, numberOfLines={}", resourceId, bytesWritten, status.getNumberOfLines());
            state = State.EXPECTING_FINISHEDMESSAGE;
            uploadAcknowledgedMessage = new UploadAcknowledgedMessage(bytesWritten, status.getNumberOfLines(), checksum);
        } catch (IOException e) {
            closeSession(e);
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
                logger.debug("Session closed: {}, resourceId={}, reason={}; marking as completed", session.getId(), resourceId, closeReason);
                manager.markCompleted(resourceId);
            } else {
                StreamingResourceTransferStatus status = manager.getStatus(resourceId).getTransferStatus();
                logger.warn("Upload session for resource {}, state {} was closed with reason {}, resource was left with status {}; marking upload as failed.", resourceId, state, closeReason, status);
                manager.markFailed(resourceId);
            }
        } else {
            logger.warn("Incomplete session (no resource ID) closed: session={}, state={}, reason={}", session.getId(), state, closeReason);
        }
    }

    @Override
    // This is invoked by the websocket framework whenever an exception occurs.
    // it will close the session (abnormally) afterward.
    public void onSessionError(Session session, Throwable throwable) {
        logger.error("Session {}, resourceId {}, state {} : unexpected error", session.getId(), resourceId, state, throwable);
    }
}
