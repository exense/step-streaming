package step.streaming.websocket.server;

import jakarta.websocket.CloseReason;
import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.*;
import step.streaming.data.MD5CalculatingInputStream;
import step.streaming.server.StreamingResourceManager;
import step.streaming.websocket.protocol.upload.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public class WebsocketUploadEndpoint extends Endpoint {
    public static final String DEFAULT_ENDPOINT_URL = "/ws/streaming/upload";


    private enum State {
        EXPECTING_METADATA,
        UPLOADING,
        FINISHED,
    }

    private static final Logger logger = LoggerFactory.getLogger(WebsocketUploadEndpoint.class);
    protected final StreamingResourceManager manager;
    private final WebsocketServerEndpointSessionsHandler sessionsHandler;

    protected Session session;
    private State state;

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
        Optional.ofNullable(sessionsHandler).ifPresent(handler -> handler.register(session));
        session.getRequestParameterMap()
                .getOrDefault(StreamingResourceUploadContext.PARAMETER_NAME, List.of())
                .stream().findFirst().ifPresent(ctx -> uploadContextId = ctx);
        state = State.EXPECTING_METADATA;
        session.addMessageHandler(String.class, this::onMessage);
        session.addMessageHandler(InputStream.class, this::onData);
        logger.debug("Session opened: {}", session.getId());
    }

    private void onMessage(String messageString) {
        if (logger.isTraceEnabled()) {
            logger.trace("Message received: {}", messageString);
        }
        UploadClientMessage clientMessage = UploadClientMessage.fromString(messageString);
        if (state == State.EXPECTING_METADATA && clientMessage instanceof RequestUploadStartMessage) {
            StreamingResourceMetadata metadata = ((RequestUploadStartMessage) clientMessage).metadata;
            try {
                resourceId = manager.registerNewResource(metadata, uploadContextId);
                StreamingResourceReference reference = manager.getReferenceFor(resourceId);
                state = State.UPLOADING;
                ReadyForUploadMessage reply = new ReadyForUploadMessage(reference);
                session.getBasicRemote().sendText(reply.toString());
            } catch (IOException e) {
                // this will implicitly activate error handling, close the session etc.
                throw new RuntimeException(e);
            }
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
            if (!status.getCurrentSize().equals(bytesWritten)) {
                throw new IllegalStateException("Unexpected size mismatch: bytesWritten=" + bytesWritten + ", but status indicates current size=" + status.getCurrentSize());
            }
            String checksum = md5Input.getChecksum();
            logger.info("Wrote {} bytes to {}, checksum={}, numberOfLines={}; sending finished message", bytesWritten, resourceId, checksum, status.getNumberOfLines());
            state = State.FINISHED;
            session.getBasicRemote().sendText(new UploadFinishedMessage(bytesWritten, status.getNumberOfLines(), checksum).toString());
        } catch (IOException e) {
            logger.error("Error while uploading data", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onClose(Session session, CloseReason closeReason) {
        Optional.ofNullable(sessionsHandler).ifPresent(handler -> handler.unregister(session));
        super.onClose(session, closeReason);
        if (resourceId != null) {
            logger.info("Session closed: {}, resourceId={}, reason={}", session.getId(), resourceId, closeReason);
            // closeReason does NOT properly implement .equals()!!!
            if (closeReason.getCloseCode() == CloseReason.CloseCodes.NORMAL_CLOSURE
                    && closeReason.getReasonPhrase().equals(UploadProtocolMessage.UPLOAD_COMPLETED)
                    && state == State.FINISHED) {
                manager.markCompleted(resourceId);
            } else {
                StreamingResourceTransferStatus status = manager.getStatus(resourceId).getTransferStatus();
                logger.warn("Upload session for resource {}, state {} was closed with reason {}, resource was left with status {}; marking upload as failed.", resourceId, state, closeReason, status);
                manager.markFailed(resourceId);
            }
        } else {
            logger.warn("Incomplete session (no resource ID) closed: session={}, state={}", session.getId(), state);
        }
    }

    @Override
    // This is invoked by the websocket framework whenever an exception occurs.
    // it will close the session (abnormally) afterward.
    public void onError(Session session, Throwable throwable) {
        logger.error("Session {}, resourceId {}, state {} : unexpected error", session.getId(), resourceId, state, throwable);
    }
}
