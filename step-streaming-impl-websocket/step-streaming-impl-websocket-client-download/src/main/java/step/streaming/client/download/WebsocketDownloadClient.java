package step.streaming.client.download;

import jakarta.websocket.ClientEndpointConfig;
import jakarta.websocket.CloseReason;
import jakarta.websocket.ContainerProvider;
import jakarta.websocket.DeploymentException;
import jakarta.websocket.EndpointConfig;
import jakarta.websocket.Session;
import jakarta.websocket.WebSocketContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.websocket.CloseReasonUtil;
import step.streaming.websocket.HalfCloseCompatibleEndpoint;
import step.streaming.websocket.protocol.download.DownloadProtocolMessage;
import step.streaming.websocket.protocol.download.DownloadServerMessage;
import step.streaming.websocket.protocol.download.LinesMessage;
import step.streaming.websocket.protocol.download.RequestChunkMessage;
import step.streaming.websocket.protocol.download.RequestLinesMessage;
import step.streaming.websocket.protocol.download.StatusChangedMessage;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class WebsocketDownloadClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WebsocketDownloadClient.class);

    private final Session session;
    private final Remote endpoint;

    // Single executor for all user callbacks (status + lines)
    private final ExecutorService callbackExecutor = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "ws-dl-client-callbacks");
        t.setDaemon(true);
        return t;
    });

    // Concurrency control for request pipelining
    private final Object lock = new Object();
    private final Queue<Runnable> requestQueue = new ArrayDeque<>();
    private boolean isRequestInFlight = false;
    private volatile boolean closed = false;

    private Consumer<InputStream> activeStreamConsumer;
    private Consumer<List<String>> activeLinesConsumer;

    private final CompletableFuture<StreamingResourceStatus> initialStatus = new CompletableFuture<>();
    private final AtomicReference<StreamingResourceStatus> lastReceivedStatus = new AtomicReference<>();
    private final List<Consumer<StreamingResourceStatus>> statusListeners = new CopyOnWriteArrayList<>();
    private final List<Runnable> closeListeners = new CopyOnWriteArrayList<>();

    public WebsocketDownloadClient(URI endpointUri) throws IOException {
        this(endpointUri, ContainerProvider.getWebSocketContainer());
    }

    public WebsocketDownloadClient(URI endpointUri, WebSocketContainer container) throws IOException {
        DownloadProtocolMessage.initialize();
        try {
            endpoint = new Remote();
            session = container.connectToServer(endpoint,
                ClientEndpointConfig.Builder.create().build(),
                endpointUri);
            logger.info("Connected to {}, waiting for initial status...", endpointUri);

            try {
                initialStatus.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new IOException("Timeout while waiting for initial status message: " + e.getMessage(), e);
            }
            logger.debug("Initial status received: {}", lastReceivedStatus.get());
        } catch (DeploymentException e) {
            throw new IOException(e);
        }
    }

    // ---- Public API --------------------------------------------------------

    public void registerStatusListener(Consumer<StreamingResourceStatus> statusListener) {
        statusListeners.add(statusListener);
        StreamingResourceStatus last = lastReceivedStatus.get();
        if (last != null) {
            safeAccept(statusListener, last);
        }
    }

    public void unregisterStatusListener(Consumer<StreamingResourceStatus> statusListener) {
        statusListeners.remove(statusListener);
    }

    public void registerCloseListener(Runnable r) {
        closeListeners.add(r);
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (closed) return;
            closed = true;
            requestQueue.clear();
        }
        if (session != null && session.isOpen()) {
            try {
                endpoint.closeSession(session, CloseReasonUtil.makeSafeCloseReason(
                    CloseReason.CloseCodes.NORMAL_CLOSURE, "Client Session closed"));
            } catch (Exception ignored) {
            }
        }
    }

    public void requestChunkStream(long startOffset, long endOffset, Consumer<InputStream> streamConsumer) {
        Objects.requireNonNull(streamConsumer, "streamConsumer");
        StreamingResourceStatus st = lastReceivedStatus.get();
        if (st == null) throw new IllegalStateException("No status yet");
        if (startOffset < 0 || startOffset > endOffset) throw new IllegalArgumentException("Bad offsets");
        if (endOffset > st.getCurrentSize())
            throw new IllegalArgumentException("endOffset " + endOffset + " > " + st.getCurrentSize());

        enqueueAndTrySend(() -> {
            synchronized (lock) {
                activeStreamConsumer = streamConsumer;
            }
            try {
                session.getBasicRemote().sendText(new RequestChunkMessage(startOffset, endOffset).toString());
            } catch (IOException e) {
                throw new RuntimeException("Failed to send chunk request", e);
            }
        });
    }

    public void requestTextLines(long startingLineIndex, long linesCount, Consumer<List<String>> linesConsumer) {
        Objects.requireNonNull(linesConsumer, "linesConsumer");
        StreamingResourceStatus st = lastReceivedStatus.get();
        if (st == null) throw new IllegalStateException("No status yet");
        if (st.getTransferStatus() == StreamingResourceTransferStatus.FAILED)
            throw new IllegalStateException("Remote resource FAILED");
        if (st.getNumberOfLines() == null)
            throw new IllegalStateException("Remote resource does not support line access");
        if (startingLineIndex < 0 || linesCount < 0)
            throw new IllegalArgumentException("Bad line params");
        if (st.getNumberOfLines() < startingLineIndex + linesCount)
            throw new IllegalArgumentException("Line request out of bounds");

        enqueueAndTrySend(() -> {
            synchronized (lock) {
                activeLinesConsumer = linesConsumer;
            }
            try {
                session.getBasicRemote().sendText(new RequestLinesMessage(startingLineIndex, linesCount).toString());
            } catch (IOException e) {
                throw new RuntimeException("Failed to send lines request", e);
            }
        });
    }

    public CompletableFuture<Long> requestChunkTransfer(long startOffset, long endOffset, OutputStream out) {
        CompletableFuture<Long> cf = new CompletableFuture<>();
        try {
            requestChunkStream(startOffset, endOffset, in -> {
                try {
                    long n = in.transferTo(out);
                    cf.complete(n);
                } catch (Exception e) {
                    cf.completeExceptionally(e);
                }
            });
        } catch (Exception e) {
            cf.completeExceptionally(e);
        }
        return cf;
    }

    // ---- Pipelining Engine -------------------------------------------------

    private void enqueueAndTrySend(Runnable sendTask) {
        synchronized (lock) {
            if (closed) throw new IllegalStateException("Client is closed");
            requestQueue.add(sendTask);
        }
        pumpQueue();
    }

    private void pumpQueue() {
        Runnable task = null;
        synchronized (lock) {
            if (!isRequestInFlight && !requestQueue.isEmpty()) {
                isRequestInFlight = true;
                task = requestQueue.poll();
            }
        }
        if (task != null) {
            try {
                task.run();
            } catch (Exception e) {
                logger.error("Error executing request task", e);
                completeCurrentRequest();
            }
        }
    }

    private void completeCurrentRequest() {
        synchronized (lock) {
            isRequestInFlight = false;
        }
        pumpQueue();
    }

    // ---- Incoming Frames ---------------------------------------------------

    private void onTextData(String messageString) {
        if (closed) return;
        DownloadServerMessage msg = DownloadServerMessage.fromString(messageString);

        if (msg instanceof StatusChangedMessage) {
            StreamingResourceStatus status = ((StatusChangedMessage) msg).resourceStatus;
            StreamingResourceStatus prev = lastReceivedStatus.getAndSet(status);

            if (prev == null || !prev.equals(status)) {
                initialStatus.complete(status);
                statusListeners.forEach(l -> callbackExecutor.execute(() -> safeAccept(l, status)));
            }
        } else if (msg instanceof LinesMessage) {
            Consumer<List<String>> consumer;
            synchronized (lock) {
                consumer = activeLinesConsumer;
                activeLinesConsumer = null;
            }
            if (consumer != null) {
                List<String> lines = ((LinesMessage) msg).lines;
                callbackExecutor.execute(() -> safeAccept(consumer, lines));
            }
            completeCurrentRequest();
        } else {
            logger.warn("Unexpected message: {}", msg);
        }
    }

    private void onStreamData(InputStream inputStream) {
        if (closed) return;

        Consumer<InputStream> consumer;
        synchronized (lock) {
            consumer = activeStreamConsumer;
            activeStreamConsumer = null;
        }

        if (consumer != null) {
            safeAccept(consumer, inputStream);
        } else {
            logger.warn("Received InputStream but no active request was found.");
        }

        try {
            inputStream.close();
        } catch (IOException ignored) {
        }

        completeCurrentRequest();
    }

    // ---- Jetty endpoint ----------------------------------------------------

    private class Remote extends HalfCloseCompatibleEndpoint {
        @Override
        public void onOpen(Session session, EndpointConfig config) {
            session.setMaxIdleTimeout(0);
            session.addMessageHandler(String.class, WebsocketDownloadClient.this::onTextData);
            session.addMessageHandler(InputStream.class, WebsocketDownloadClient.this::onStreamData);
        }

        @Override
        public void onSessionClose(Session session, CloseReason closeReason) {
            logger.info("[onSessionClose] {}", closeReason);
            synchronized (lock) {
                closed = true;
                requestQueue.clear();
            }
            for (Runnable r : closeListeners) safeRun(r);
            callbackExecutor.shutdown();
        }

        @Override
        public void onSessionError(Session session, Throwable thr) {
            logger.warn("[onSessionError] {}", thr.toString(), thr);
        }
    }

    // ---- Helpers -----------------------------------------------------------

    private static <T> void safeAccept(Consumer<T> c, T v) {
        try {
            c.accept(v);
        } catch (RuntimeException ex) {
            throw new RuntimeException("Endpoint notification error", ex);
        }
    }

    private static void safeRun(Runnable r) {
        try {
            r.run();
        } catch (RuntimeException ignored) {
        }
    }
}
