package step.streaming.client.download;

import jakarta.websocket.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.websocket.protocol.download.DownloadProtocolMessage;
import step.streaming.websocket.protocol.download.DownloadServerMessage;
import step.streaming.websocket.protocol.download.RequestChunkMessage;
import step.streaming.websocket.protocol.download.StatusChangedMessage;

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
    private State state;
    private Consumer<InputStream> dataConsumer;

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
            return container.connectToServer(new Remote(), ClientEndpointConfig.Builder.create().build(), websocketUri);
        } catch (DeploymentException e) {
            throw new IOException(e);
        }
    }

    private void onMessage(String messageString) {
        if (state == State.CONNECTED) {
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
        } else {
            throw new IllegalStateException("Unexpected message in state " + state + ": " + message);
        }
    }

    private void onData(InputStream inputStream) {
        if (state != State.AWAITING_DOWNLOAD) {
            throw new IllegalStateException("Expected state AWAITING_DOWNLOAD, got " + state);
        }
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

    public void requestChunkStream(long startOffset, long endOffset, Consumer<InputStream> streamConsumer) throws Exception {
        if (state != State.READY) {
            throw new IllegalStateException("Not in READY state (current state: " + state + ")");
        }
        // FIXME: be more strict here?
        if (streamConsumer == null) {
            logger.warn("streamConsumer is null, data will be transferred but discarded");
        }
        if (startOffset < 0 || startOffset > endOffset) {
            throw new IllegalArgumentException("Invalid offsets: " + startOffset + ", " + endOffset);
        }
        StreamingResourceStatus status = lastReceivedStatus.get();
        if (status.getTransferStatus() == StreamingResourceTransferStatus.FAILED) {
            throw new IllegalStateException("Remote resource indicates a FAILED resource");
        }
        if (endOffset > status.getCurrentSize()) {
            throw new IllegalArgumentException("Invalid end offset: " + endOffset + " > " + status.getCurrentSize());
        }
        state = State.AWAITING_DOWNLOAD;
        dataConsumer = streamConsumer;
        logger.debug("{} requesting chunk [{}, {}]", this, startOffset, endOffset);
        session.getBasicRemote().sendText(new RequestChunkMessage(startOffset, endOffset).toString());
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
    public void close() throws Exception {
        if (state != State.CLOSED) {
            if (session.isOpen()) {
                if (state != State.READY) {
                    logger.warn("Closing session while in state {}, this may cause erroneous behaviour", state);
                }
                session.close(new CloseReason(CloseReason.CloseCodes.NORMAL_CLOSURE, "Client Session closed"));
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

    private class Remote extends Endpoint {
        @Override
        public void onOpen(Session session, EndpointConfig config) {
            // No timeout; server side takes care of keepalive messages
            session.setMaxIdleTimeout(0);
            session.addMessageHandler(String.class, self::onMessage);
            session.addMessageHandler(InputStream.class, self::onData);
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
