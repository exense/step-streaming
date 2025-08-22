package step.streaming.client.download;

import jakarta.websocket.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
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
        CONNECTED,
        READY,
        AWAITING_DOWNLOAD,
        DOWNLOADING,
        CLOSED,
    }

    private final Session session;
    private final WebsocketDownloadClient self = this;
    private final CompletableFuture<StreamingResourceStatus> initialStatus = new CompletableFuture<>();
    private final AtomicReference<StreamingResourceStatus> lastReceivedStatus = new AtomicReference<>();
    private final List<Consumer<StreamingResourceStatus>> statusListeners = new CopyOnWriteArrayList<>(
            List.of(initialStatus::complete, lastReceivedStatus::set));
    private final List<Runnable> closeListeners = new CopyOnWriteArrayList<>();
    private Remote endpoint;
    private State state;
    private Consumer<InputStream> dataConsumer;
    private Consumer<List<String>> linesConsumer;

    public WebsocketDownloadClient(URI endpointUri) throws IOException {
        this(endpointUri, ContainerProvider.getWebSocketContainer());
    }

    public WebsocketDownloadClient(URI endpointUri, WebSocketContainer container) throws IOException {
        DownloadProtocolMessage.initialize();
        session = connect(endpointUri, container);
        state = State.CONNECTED;
        logger.debug("{} Connected to endpoint {}, awaiting first status", this, endpointUri);
        try {
            initialStatus.get(2, TimeUnit.SECONDS);
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
            // we're only ready after receiving an initial status message from the server
            state = State.READY;
        }
        logger.debug("{} Received message {}", this, messageString);
        DownloadServerMessage message = DownloadServerMessage.fromString(messageString);
        if (message instanceof StatusChangedMessage) {
            StreamingResourceStatus status = ((StatusChangedMessage) message).resourceStatus;
            if (!status.equals(lastReceivedStatus.get())) { // debounce: avoid sending the same status twice, just in case
                logger.debug("Notifying listeners, status = {}", status);
                for (Consumer<StreamingResourceStatus> statusListener : statusListeners) {
                    statusListener.accept(status);
                }
            } else {
                logger.debug("Omitting notification for unchanged status {}", status);
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
        expectCurrentState(State.AWAITING_DOWNLOAD);
        state = State.DOWNLOADING;
        if (dataConsumer != null) {
            dataConsumer.accept(inputStream);
        }
        // Ensure the stream is closed. Exceptions are non-critical.
        try {
            inputStream.close();
        } catch (IOException ignored) {
        }
        dataConsumer = null;
        state = State.READY;
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
    }

    public void requestChunkStream(long startOffset, long endOffset, Consumer<InputStream> streamConsumer) throws Exception {
        expectCurrentState(State.READY);
        // FIXME: be more strict here?
        if (streamConsumer == null) {
            logger.warn("streamConsumer is null, data will be transferred but discarded");
        }
        if (startOffset < 0 || startOffset > endOffset) {
            throw new IllegalArgumentException("Invalid offsets: " + startOffset + ", " + endOffset);
        }
        StreamingResourceStatus status = lastReceivedStatus.get();
        if (endOffset > status.getCurrentSize()) {
            throw new IllegalArgumentException("Invalid end offset: " + endOffset + " > " + status.getCurrentSize());
        }
        state = State.AWAITING_DOWNLOAD;
        dataConsumer = streamConsumer;
        logger.debug("{} requesting chunk [{}, {}]", this, startOffset, endOffset);
        session.getBasicRemote().sendText(new RequestChunkMessage(startOffset, endOffset).toString());
    }

    public void requestTextLines(long startingLineIndex, long linesCount, Consumer<List<String>> linesConsumer) throws Exception {
        expectCurrentState(State.READY);
        if (startingLineIndex < 0) {
            throw new IllegalArgumentException("Invalid startingLineIndex: " + startingLineIndex);
        }
        if (linesCount < 0) {
            throw new IllegalArgumentException("Invalid linesCount: " + linesCount);
        }
        StreamingResourceStatus status = lastReceivedStatus.get();
        if (status.getTransferStatus() == StreamingResourceTransferStatus.FAILED) {
            throw new IllegalStateException("Remote resource indicates a FAILED resource");
        }
        if (status.getNumberOfLines() == null) {
            throw new IllegalStateException("Remote resource does not support access by line number");
        }
        if (status.getNumberOfLines() < startingLineIndex + linesCount ) {
            throw new IllegalArgumentException("Line access request out of bounds");
        }
        state = State.AWAITING_DOWNLOAD;
        this.linesConsumer = linesConsumer;
        logger.debug("{} requesting lines {start={}, count={}}", this, startingLineIndex, linesCount);
        session.getBasicRemote().sendText(new RequestLinesMessage(startingLineIndex, linesCount).toString());

    }



    public CompletableFuture<Long> requestChunkTransfer(long startOffset, long endOffset, OutputStream outputStream) {
        CompletableFuture<Long> transferredFuture = new CompletableFuture<>();
        Consumer<InputStream> streamConsumer = inputStream -> {
            try {
                long written = inputStream.transferTo(outputStream);
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
                endpoint.closeSession(session, new CloseReason(CloseReason.CloseCodes.NORMAL_CLOSURE, "Client Session closed"));
            }
            state = State.CLOSED;
        }
    }

    public void registerStatusListener(Consumer<StreamingResourceStatus> statusListener) {
        statusListeners.add(statusListener);
        if (lastReceivedStatus.get() != null) {
            statusListener.accept(lastReceivedStatus.get());
        }
    }

    public void unregisterStatusListener(Consumer<StreamingResourceStatus> statusListener) {
        statusListeners.remove(statusListener);
    }

    private class Remote extends HalfCloseCompatibleEndpoint {
        @Override
        public void onOpen(Session session, EndpointConfig config) {
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

}
