package step.streaming.websocket.server;

import jakarta.websocket.CloseReason;
import jakarta.websocket.Endpoint;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.data.CheckpointingOutputStream;
import step.streaming.server.StreamingResourceManager;
import step.streaming.websocket.protocol.download.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WebsocketDownloadEndpoint extends Endpoint {
    public static final String DEFAULT_ENDPOINT_URL = "/ws/streaming/download/{id}";
    public static final String DEFAULT_PARAMETER_NAME = "id";

    private static final Logger logger = LoggerFactory.getLogger(WebsocketDownloadEndpoint.class);
    private final WebsocketServerEndpointSessionsHandler sessionsHandler;
    protected final StreamingResourceManager manager;
    protected final String resourceIdParameterName;
    // we must use the same reference for registering/unregistering
    private final Consumer<StreamingResourceStatus> statusChangeListener = this::onResourceStatusChanged;

    // The following three fields work together to send (asynchronously received) status updates
    // in a synchronous fashion.
    private final Object coordinationLock = new Object();
    private boolean downloading = false;
    private final AtomicReference<StreamingResourceStatus> deferredStatus = new AtomicReference<>();



    protected Session session;
    protected String resourceId;

    public WebsocketDownloadEndpoint(StreamingResourceManager manager, WebsocketServerEndpointSessionsHandler sessionsHandler, String resourceIdParameterName) {
        DownloadProtocolMessage.initialize();
        this.manager = manager;
        this.sessionsHandler = sessionsHandler;
        this.resourceIdParameterName = resourceIdParameterName;
    }

    @Override
    public void onOpen(Session session, EndpointConfig config) {
        this.session = session;
        Optional.ofNullable(sessionsHandler).ifPresent(h -> h.register(session));
        session.addMessageHandler(String.class, this::onMessage);
        resourceId = Objects.requireNonNull(session.getPathParameters().get(resourceIdParameterName));
        logger.debug("Session opened: {}, resource={}", session.getId(), resourceId);
        manager.registerStatusListener(resourceId, statusChangeListener);
    }

    private void onMessage(String messageString) {
        if (logger.isTraceEnabled()) {
            logger.trace("Message received: {}", messageString);
        }
        DownloadClientMessage clientMessage = DownloadClientMessage.fromString(messageString);
        if (clientMessage instanceof RequestChunkMessage) {
            handleDownloadRequest((RequestChunkMessage) clientMessage);
        } else if (clientMessage instanceof RequestLinesMessage) {
            handleLinesRequest((RequestLinesMessage) clientMessage);
        } else {
            throw new IllegalStateException("Unhandled message: " + clientMessage);
        }
    }

    private void onResourceStatusChanged(StreamingResourceStatus newStatus) {
        synchronized (coordinationLock) {
            if (downloading) {
                logger.debug("Download in progress, deferring status update");
                // It's OK to only keep the latest status, in case multiple ones would arrive and be deferred
                deferredStatus.set(newStatus);
            } else {
                sendStatusUpdate(newStatus);
            }
        }
    }

    private void enterDownloadingState() {
        synchronized (coordinationLock) {
            downloading = true;
        }
    }

    private void exitDownloadingState() {
        synchronized (coordinationLock) {
            downloading = false;
            // Check if a status update was deferred during the download
            StreamingResourceStatus deferred = deferredStatus.getAndSet(null);
            if (deferred != null) {
                logger.debug("Sending deferred status update after download");
                sendStatusUpdate(deferred);
            }
        }
    }

    private void handleDownloadRequest(RequestChunkMessage request) {
        enterDownloadingState();
        logger.debug("Received request for chunk [{}, {}] of resource {}", request.startOffset, request.endOffset, resourceId);
        try (InputStream in = manager.openStream(resourceId, request.startOffset, request.endOffset);
             OutputStream out = new CheckpointingOutputStream(session.getBasicRemote().getSendStream(), 500, null)) {
            long sent = in.transferTo(out);
            logger.debug("{} {} Transfer completed: {} bytes", session.getId(), resourceId, sent);
        } catch (IOException e) {
            logger.error("Error while sending chunk [{}, {}] of resource {}", request.startOffset, request.endOffset, e);
            throw new RuntimeException(e);
        }
        exitDownloadingState();
    }

    private void handleLinesRequest(RequestLinesMessage request) {
        enterDownloadingState();
        logger.debug("Received request for {} lines starting at line index {} for resource {}", request.linesCount, request.startingLineIndex, resourceId);
        try {
            Stream<String> linesStream = manager.getLines(resourceId, request.startingLineIndex, request.linesCount);
            List<String> linesList = linesStream.collect(Collectors.toList());
            logger.debug("Sent {} lines from resource {}", linesList.size(), resourceId);
            session.getBasicRemote().sendText(new LinesMessage(linesList).toString());
        } catch (Exception e) {
            logger.error("Error while getting lines (start={}, count={}) from resource {}", request.startingLineIndex, request.linesCount, resourceId, e);
            throw new RuntimeException(e);
        }
        exitDownloadingState();
    }

    private void sendStatusUpdate(StreamingResourceStatus status) {
        String message = new StatusChangedMessage(status).toString();
        logger.debug("Notifying client endpoint about status change: {}", message);
        try {
            session.getBasicRemote().sendText(message);
            logger.debug("Notification sent.");
        } catch (IOException e) {
            logger.error("Error notifying client endpoint about status change", e);
        }
    }


    @Override
    public void onClose(Session session, CloseReason closeReason) {
        logger.debug("Session closed: {}, resource={}, reason={}", session.getId(), resourceId, closeReason);
        manager.unregisterStatusListener(resourceId, statusChangeListener);
        Optional.ofNullable(sessionsHandler).ifPresent(h -> h.unregister(session));
    }

    @Override
    public void onError(Session session, Throwable thr) {
        super.onError(session, thr);
    }
}
