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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class WebsocketDownloadClient implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(WebsocketDownloadClient.class);

    private enum State {CREATED, READY, AWAITING_DOWNLOAD, DOWNLOADING, CLOSED}

    private final Session session;
    private final WebsocketDownloadClient self = this;

    // Single-threaded event loop for control plane
    private final ExecutorService eventLoop = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "ws-dl-client-loop");
        t.setDaemon(true);
        return t;
    });

    // Separate executor for user callbacks (status + lines)
    private final ExecutorService deliveryExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "ws-dl-client-delivery");
        t.setDaemon(true);
        return t;
    });

    private volatile State state = State.CREATED;
    private volatile boolean busy;

    // Request queue (serialized on event loop)
    private abstract static class Request {
        abstract void send(Session s) throws Exception;

        abstract String hint();
    }

    private final Deque<Request> requests = new ArrayDeque<>();
    private final AtomicLong requestIds = new AtomicLong();
    private volatile long inFlightReqId;

    // Status plumbing
    private final CompletableFuture<StreamingResourceStatus> initialStatus = new CompletableFuture<>();
    private final AtomicReference<StreamingResourceStatus> lastReceivedStatus = new AtomicReference<>();
    private final List<Consumer<StreamingResourceStatus>> statusListeners = new CopyOnWriteArrayList<>();

    // Close listeners
    private final List<Runnable> closeListeners = new CopyOnWriteArrayList<>();

    private final Remote endpoint;
    private Consumer<InputStream> dataConsumer;
    private Consumer<List<String>> linesConsumer;

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
        logger.debug("StatusListener registered, synchronously sending status unless null: {}", last);
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
        runOnLoop(() -> {
            logger.debug("[CLOSE called] state={}, busy={}, q={}", state, busy, requests.size());
            if (state == State.CLOSED) return;
            state = State.CLOSED;
            if (session.isOpen()) {
                endpoint.closeSession(session, CloseReasonUtil.makeSafeCloseReason(
                    CloseReason.CloseCodes.NORMAL_CLOSURE, "Client Session closed"));
            }
            requests.clear();
            // We will shut down the executors in onSessionClose after fan-out.
        });
    }

    public void requestChunkStream(long startOffset, long endOffset, Consumer<InputStream> streamConsumer) {
        Objects.requireNonNull(streamConsumer, "streamConsumer");
        runOnLoop(() -> {
            requireOpen();
            StreamingResourceStatus st = lastReceivedStatus.get();
            if (st == null) throw new IllegalStateException("No status yet");
            if (startOffset < 0 || startOffset > endOffset) throw new IllegalArgumentException("Bad offsets");
            if (endOffset > st.getCurrentSize())
                throw new IllegalArgumentException("endOffset " + endOffset + " > " + st.getCurrentSize());

            long id = requestIds.incrementAndGet();
            Request req = new Request() {
                @Override
                void send(Session s) throws Exception {
                    state = State.AWAITING_DOWNLOAD;
                    inFlightReqId = id;
                    dataConsumer = streamConsumer;
                    logger.debug("[SEND chunk {} PRE] [{}] state={}, q={}", id, hint(), state, requests.size());
                    s.getBasicRemote().sendText(new RequestChunkMessage(startOffset, endOffset).toString());
                    logger.debug("[SEND chunk {} POST] [{}] state={}, q={}", id, hint(), state, requests.size());
                }

                @Override
                String hint() {
                    return "chunk[" + startOffset + "," + endOffset + "]";
                }
            };
            enqueueOrStart(req);
        });
    }

    public void requestTextLines(long startingLineIndex, long linesCount, Consumer<List<String>> linesConsumer) {
        Objects.requireNonNull(linesConsumer, "linesConsumer");
        runOnLoop(() -> {
            requireOpen();
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

            long id = requestIds.incrementAndGet();
            Request req = new Request() {
                @Override
                void send(Session s) throws Exception {
                    state = State.AWAITING_DOWNLOAD;
                    inFlightReqId = id;
                    WebsocketDownloadClient.this.linesConsumer = linesConsumer;
                    logger.debug("[SEND lines {} PRE] [{}] state={}, q={}", id, hint(), state, requests.size());
                    s.getBasicRemote().sendText(new RequestLinesMessage(startingLineIndex, linesCount).toString());
                    logger.debug("[SEND lines {} POST] [{}] state={}, q={}", id, hint(), state, requests.size());
                }

                @Override
                String hint() {
                    return "lines[start=" + startingLineIndex + ",count=" + linesCount + "]";
                }
            };
            enqueueOrStart(req);
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

    // ---- Jetty endpoint ----------------------------------------------------

    private class Remote extends HalfCloseCompatibleEndpoint {
        @Override
        public void onOpen(Session session, EndpointConfig config) {
            session.setMaxIdleTimeout(0);
            session.addMessageHandler(String.class, self::onTextData);
            session.addMessageHandler(InputStream.class, self::onStreamData);
            state = State.READY; // visible state before first message
            logger.debug("[onOpen] state set to READY");
        }

        @Override
        public void onSessionClose(Session session, CloseReason closeReason) {
            runOnLoop(() -> {
                logger.info("[onSessionClose] {}", closeReason);
                state = State.CLOSED;
                requests.clear();
                for (Runnable r : closeListeners) safeRun(r);
                // Now that we’re closed and fan-out is done, stop executors.
                eventLoop.shutdown();
                deliveryExecutor.shutdown();
            });
        }

        @Override
        public void onSessionError(Session session, Throwable thr) {
            logger.warn("[onSessionError] {}", thr.toString(), thr);
        }
    }

    // ---- Incoming frames ---------------------------------------------------

    private void onTextData(String messageString) {
        runOnLoop(() -> {
            if (state == State.CLOSED) return;
            logger.debug("[TEXT IN] state={}, busy={}, q={}, inFlightReqId={}, loop=ws-dl-client-loop",
                state, busy, requests.size(), inFlightReqId);
            DownloadServerMessage msg = DownloadServerMessage.fromString(messageString);

            if (msg instanceof StatusChangedMessage) {
                StreamingResourceStatus status = ((StatusChangedMessage) msg).resourceStatus;
                if (!initialStatus.isDone()) {
                    initialStatus.complete(status);
                }
                StreamingResourceStatus prev = lastReceivedStatus.getAndSet(status);
                if (prev == null || !prev.equals(status)) {
                    logger.debug("Fan-out status {}", status);
                    for (Consumer<StreamingResourceStatus> l : statusListeners) {
                        deliveryExecutor.execute(() -> safeAccept(l, status));
                    }
                } else {
                    logger.debug("Status dedup {}", status);
                }

            } else if (msg instanceof LinesMessage) {
                logger.debug("LINES IN, expecting AWAITING_DOWNLOAD, state={}", state);
                try {
                    if (state != State.AWAITING_DOWNLOAD)
                        throw new IllegalStateException("Unexpected lines when state=" + state);
                    state = State.READY;
                    Consumer<List<String>> c = this.linesConsumer;
                    this.linesConsumer = null;
                    if (c != null) {
                        List<String> lines = ((LinesMessage) msg).lines;
                        logger.debug("Delivering {} lines to consumer", lines.size());
                        deliveryExecutor.execute(() -> safeAccept(c, lines));
                    }
                } finally {
                    finishCurrent();
                }

            } else {
                throw new IllegalStateException("Unexpected message: " + msg);
            }
        });
    }

    // Binary frames (data plane)
    private void onStreamData(InputStream inputStream) {
        if (state != State.AWAITING_DOWNLOAD) {
            logger.debug("Unexpected InputStream while state={}", state);
        }
        state = State.DOWNLOADING;
        if (dataConsumer != null) {
            dataConsumer.accept(inputStream);
        }
        try {
            inputStream.close();
        } catch (IOException ignored) {
        }
        dataConsumer = null;
        runOnLoop(this::finishCurrent);
    }

    // ---- Event-loop helpers ------------------------------------------------

    private void enqueueOrStart(Request req) {
        if (busy) {
            requests.addLast(req);
            logger.debug("[ENQ] [{}] now q={}", req.hint(), requests.size());
        } else {
            busy = true;
            sendNow(req);
        }
    }

    private void sendNow(Request req) {
        try {
            inFlightReqId = requestIds.get();
            logger.debug("[SEND] [{}] start, state={}, q={}", req.hint(), state, requests.size());
            req.send(session);
            logger.debug("[SEND] [{}] done, state={}, q={}", req.hint(), state, requests.size());
        } catch (Exception e) {
            busy = false;
            throw new RuntimeException(e);
        }
    }

    private void finishCurrent() {
        logger.debug("[FINISH start] state={}, busy={}, queueSize={}, inFlightReqId={}",
            state, busy, requests.size(), inFlightReqId);
        state = State.READY;
        Request next = requests.pollFirst();
        if (next != null) {
            logger.debug("[FINISH startNext -> {}] state={}, q={}", next.hint(), state, requests.size());
            sendNow(next);
        } else {
            busy = false;
            inFlightReqId = 0L;
            logger.debug("[FINISH idle] state={}, busy={}, queueSize={}", state, busy, requests.size());
        }
    }

    private void runOnLoop(Runnable r) {
        try {
            if (!eventLoop.isShutdown()) {
                eventLoop.execute(r);
            } else {
                logger.debug("[runOnLoop] loop is shut down; dropping task");
            }
        } catch (RejectedExecutionException rex) {
            logger.debug("[runOnLoop] task rejected (loop terminated): {}", rex.toString());
        }
    }

    private void requireOpen() {
        if (state == State.CLOSED) throw new IllegalStateException("Client closed");
    }

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
        } catch (RuntimeException ex) {
            // swallow; listener bugs shouldn’t kill us
        }
    }
}
