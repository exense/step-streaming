package step.streaming.client.download;

import jakarta.websocket.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.websocket.CloseReasonUtil;
import step.streaming.websocket.HalfCloseCompatibleEndpoint;
import step.streaming.websocket.protocol.download.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Class providing the actual low-level implementation for Websocket downloads.
 * Use by the higher-level, user-facing {@link WebsocketDownload} class.
 */
public class WebsocketDownloadClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WebsocketDownloadClient.class);

    private enum State {
        VIRGIN,
        CONNECTED,
        READY,
        AWAITING_DOWNLOAD,
        DOWNLOADING,
        CLOSED,
    }

    private final Session session;
    private final WebsocketDownloadClient self = this;

    // Manage initial/last status explicitly; user listeners only.
    private final CompletableFuture<StreamingResourceStatus> initialStatus = new CompletableFuture<>();
    private final AtomicReference<StreamingResourceStatus> lastReceivedStatus = new AtomicReference<>();
    private final List<Consumer<StreamingResourceStatus>> statusListeners = new CopyOnWriteArrayList<>();

    private final List<Runnable> closeListeners = new CopyOnWriteArrayList<>();
    private Remote endpoint;
    private volatile State state = State.VIRGIN;
    private Consumer<InputStream> dataConsumer;
    private Consumer<List<String>> linesConsumer;

    // allow deferring exactly one request while a download is in-flight
    private final AtomicReference<Runnable> pendingAction = new AtomicReference<>();

    // defer status messages that arrive while not READY
    private final AtomicReference<StreamingResourceStatus> deferredStatus =
            new AtomicReference<>();

    public WebsocketDownloadClient(URI endpointUri) throws IOException {
        this(endpointUri, ContainerProvider.getWebSocketContainer());
    }

    public WebsocketDownloadClient(URI endpointUri, WebSocketContainer container) throws IOException {
        DownloadProtocolMessage.initialize();
        session = connect(endpointUri, container);
        logger.debug("{} Connected to endpoint {}, awaiting first status", this, endpointUri);
        try {
            initialStatus.get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IOException("Timeout while waiting for initial status message: " + e.getMessage(), e);
        }
    }

    private Session connect(URI websocketUri, WebSocketContainer container) throws IOException {
        try {
            endpoint = new Remote();
            return container.connectToServer(endpoint, ClientEndpointConfig.Builder.create().build(), websocketUri);
        } catch (DeploymentException e) {
            throw new IOException(e);
        }
    }

    private void onTextData(String messageString) {
        if (state == State.CONNECTED) {
            // we're only ready after receiving an initial message from the server
            state = State.READY;
        }
        logger.debug("{} Received message {}", this, messageString);
        DownloadServerMessage message = DownloadServerMessage.fromString(messageString);
        if (message instanceof StatusChangedMessage) {
            StreamingResourceStatus status = ((StatusChangedMessage) message).resourceStatus;
            if (!initialStatus.isDone()) {
                logger.debug("Setting initial status to {}", status);
                initialStatus.complete(status);
            }

            // If we're busy with a request, defer notifying listeners until we're READY again
            if (state == State.AWAITING_DOWNLOAD || state == State.DOWNLOADING) {
                deferredStatus.set(status);
                logger.debug("Deferring status {} because state is {}", status, state);
                return;
            }

            // update lastReceivedStatus first; value-based dedupe
            StreamingResourceStatus prev = lastReceivedStatus.getAndSet(status);
            if (prev != null && prev.equals(status)) {
                logger.debug("Omitting notification for unchanged status {}", status);
                if (state == State.READY) {
                    runPendingIfAny();
                }
                return;
            }

            logger.debug("Notifying listeners, status = {}", status);
            for (Consumer<StreamingResourceStatus> statusListener : statusListeners) {
                try {
                    statusListener.accept(status);
                } catch (RuntimeException ex) {
                    // propagate as a close reason-friendly error (keeps Jetty happy)
                    throw new RuntimeException("Endpoint notification error", ex);
                }
            }
            if (state == State.READY) {
                runPendingIfAny();
            }
        } else if (message instanceof LinesMessage) {
            onLinesData(((LinesMessage) message).lines);
        } // note: binary data arrives through the onStreamData hook
        else {
            throw new IllegalStateException("Unexpected message in state " + state + ": " + message);
        }
    }

    private void expectCurrentState(State expectedState) {
        if (state != expectedState) {
            throw new IllegalStateException("Expected to be in state " + expectedState + " but was " + state);
        }
    }

    private void onStreamData(InputStream inputStream) {
        logger.debug("Received input stream, starting download");
        expectCurrentState(State.AWAITING_DOWNLOAD);
        state = State.DOWNLOADING;
        if (dataConsumer != null) {
            logger.debug("dataConsumer present, giving it the stream");
            dataConsumer.accept(inputStream);
        } else {
            logger.debug("No data consumer!");
        }
        // Ensure the stream is closed. Exceptions are non-critical.
        try {
            inputStream.close();
        } catch (IOException ignored) {
        }
        logger.debug("stream consumed, resetting status");
        dataConsumer = null;
        state = State.READY;
        logger.debug("running pending tasks if present and draining potential deferred status message");
        runPendingIfAny();
        drainDeferredStatusIfAny();
        // run again in case the pending action was set during draining
        runPendingIfAny();
    }

    private void onLinesData(List<String> lines) {
        expectCurrentState(State.AWAITING_DOWNLOAD);
        if (linesConsumer != null) {
            try {
                linesConsumer.accept(lines);
            } catch (Exception unexpected) {
                logger.error("Unexpected error while handling lines request callback", unexpected);
            }
        }
        state = State.READY;
        runPendingIfAny();
        drainDeferredStatusIfAny(); // NEW: deliver any deferred status now
        // run again in case the pending action was set during draining
        runPendingIfAny();
    }

    // defer if not READY (single-flight). Otherwise send immediately.
    public void requestChunkStream(long startOffset, long endOffset, Consumer<InputStream> streamConsumer) throws Exception {
        logger.debug("Requesting chunk stream for [{}, {}]", startOffset, endOffset);
        if (state == State.READY) {
            logger.debug("State is READY, sending request");
            sendChunkRequest(startOffset, endOffset, streamConsumer);
            return;
        }
        logger.debug("State not READY, preparing deferred action");
        Runnable action = () -> {
            try {
                sendChunkRequest(startOffset, endOffset, streamConsumer);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        if (!pendingAction.compareAndSet(null, action)) {
            throw new IllegalStateException("Another download request is already pending");
        }
        logger.debug("pendingAction was set, checking again if we're READY now?");
        if (state == State.READY && pendingAction.compareAndSet(action, null)) {
            logger.debug("Became READY after deferring; running pending action now");
            action.run();
        }
    }

    // defer if not READY (single-flight). Otherwise send immediately.
    public void requestTextLines(long startingLineIndex, long linesCount, Consumer<List<String>> linesConsumer) throws Exception {
        if (state == State.READY) {
            sendLinesRequest(startingLineIndex, linesCount, linesConsumer);
            return;
        }
        Runnable action = () -> {
            try {
                sendLinesRequest(startingLineIndex, linesCount, linesConsumer);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        if (!pendingAction.compareAndSet(null, action)) {
            throw new IllegalStateException("Another download request is already pending");
        }
        if (state == State.READY && pendingAction.compareAndSet(action, null)) {
            action.run();
        }
    }

    public CompletableFuture<Long> requestChunkTransfer(long startOffset, long endOffset, OutputStream outputStream) {
        logger.debug("transfer request for [{}, {}]", startOffset, endOffset);
        CompletableFuture<Long> transferredFuture = new CompletableFuture<>();
        Consumer<InputStream> streamConsumer = inputStream -> {
            try {
                logger.debug("InputStream received, awaiting bytes");
                long written = inputStream.transferTo(outputStream);
                logger.debug("wrote {} bytes", written);
                inputStream.close();
                transferredFuture.complete(written);
            } catch (Exception e) {
                transferredFuture.completeExceptionally(e);
            }
        };
        try {
            requestChunkStream(startOffset, endOffset, streamConsumer);
        } catch (Exception e) {
            transferredFuture.completeExceptionally(e);
        }
        return transferredFuture;
    }

    private void onError(Throwable throwable) {
        logger.error(throwable.getMessage(), throwable);
    }

    public void registerCloseListener(Runnable runnable) {
        closeListeners.add(runnable);
    }

    private void onClose(CloseReason closeReason) {
        if (state == State.READY || state == State.CLOSED) {
            logger.debug("Session closed: {}", closeReason);
        } else {
            logger.warn("Session closed while in state {}: {}", state, closeReason);
        }
        closeListeners.forEach(Runnable::run);
        state = State.CLOSED;
    }

    @Override
    public void close() throws IOException {
        if (state != State.CLOSED) {
            if (session.isOpen()) {
                if (state != State.READY) {
                    logger.warn("Closing session while in state {}, this may cause erroneous behaviour", state);
                }
                endpoint.closeSession(session, CloseReasonUtil.makeSafeCloseReason(CloseReason.CloseCodes.NORMAL_CLOSURE, "Client Session closed"));
            }
            state = State.CLOSED;
        }
    }

    public void registerStatusListener(Consumer<StreamingResourceStatus> statusListener) {
        statusListeners.add(statusListener);
        StreamingResourceStatus last = lastReceivedStatus.get();
        if (last != null) {
            statusListener.accept(last);
        }
    }

    public void unregisterStatusListener(Consumer<StreamingResourceStatus> statusListener) {
        statusListeners.remove(statusListener);
    }

    private class Remote extends HalfCloseCompatibleEndpoint {
        @Override
        public void onOpen(Session session, EndpointConfig config) {
            self.state = State.CONNECTED;
            // No timeout; server side takes care of keepalive messages
            session.setMaxIdleTimeout(0);
            session.addMessageHandler(String.class, self::onTextData);
            session.addMessageHandler(InputStream.class, self::onStreamData);
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

    // internal, assumes state == READY
    private void sendChunkRequest(long startOffset, long endOffset, Consumer<InputStream> streamConsumer) throws Exception {
        if (streamConsumer == null) {
            logger.warn("streamConsumer is null, data will be transferred but discarded");
        }
        if (startOffset < 0 || startOffset > endOffset) {
            throw new IllegalArgumentException("Invalid offsets: " + startOffset + ", " + endOffset);
        }
        StreamingResourceStatus status = lastReceivedStatus.get();
        if (status == null || endOffset > status.getCurrentSize()) {
            throw new IllegalArgumentException("Invalid end offset: " + endOffset + " > " + (status != null ? status.getCurrentSize() : -1));
        }
        state = State.AWAITING_DOWNLOAD;
        dataConsumer = streamConsumer;
        logger.debug("{} requesting chunk [{}, {}]", this, startOffset, endOffset);
        session.getBasicRemote().sendText(new RequestChunkMessage(startOffset, endOffset).toString());
    }

    // internal, assumes state == READY
    private void sendLinesRequest(long startingLineIndex, long linesCount, Consumer<List<String>> linesConsumer) throws Exception {
        if (startingLineIndex < 0) {
            throw new IllegalArgumentException("Invalid startingLineIndex: " + startingLineIndex);
        }
        if (linesCount < 0) {
            throw new IllegalArgumentException("Invalid linesCount: " + linesCount);
        }
        StreamingResourceStatus status = lastReceivedStatus.get();
        if (status == null) {
            throw new IllegalStateException("No status available yet");
        }
        if (status.getTransferStatus() == StreamingResourceTransferStatus.FAILED) {
            throw new IllegalStateException("Remote resource indicates a FAILED resource");
        }
        if (status.getNumberOfLines() == null) {
            throw new IllegalStateException("Remote resource does not support access by line number");
        }
        if (status.getNumberOfLines() < startingLineIndex + linesCount) {
            throw new IllegalArgumentException("Line access request out of bounds");
        }
        state = State.AWAITING_DOWNLOAD;
        this.linesConsumer = linesConsumer;
        logger.debug("{} requesting lines {start={}, count={}}", this, startingLineIndex, linesCount);
        session.getBasicRemote().sendText(new RequestLinesMessage(startingLineIndex, linesCount).toString());
    }

    // run and clear any deferred action (if present)
    private void runPendingIfAny() {
        logger.debug("Checking for pending action");
        Runnable r = pendingAction.getAndSet(null);
        if (r != null) {
            logger.debug("Found pending action, running it now");
            r.run();
        } else {
            logger.debug("No pending action found");
        }
    }

    // deliver deferred status once we’re READY again
    private void drainDeferredStatusIfAny() {
        StreamingResourceStatus status = deferredStatus.getAndSet(null);
        if (status == null) return;

        logger.debug("Found deferred status message: {}", status);
        StreamingResourceStatus prev = lastReceivedStatus.getAndSet(status);
        if (prev == null) {
            logger.debug("Setting initial status: {}", status);
            initialStatus.complete(status);
        }
        if (prev != null && prev.equals(status)) {
            logger.debug("Omitting notification for unchanged deferred status {}", status);
            return;
        }
        logger.debug("Delivering deferred status {}", status);
        for (Consumer<StreamingResourceStatus> statusListener : statusListeners) {
            try {
                statusListener.accept(status);
            } catch (RuntimeException ex) {
                throw new RuntimeException("Endpoint notification error", ex);
            }
        }
    }
}
