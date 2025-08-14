package step.streaming.client.download;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.client.AbstractStreamingTransfer;
import step.streaming.common.StreamingResourceReference;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.data.DelegatingInputStream;
import step.streaming.data.TrackablePipedInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * A {@link StreamingDownload} implementation that handles downloading a resource over WebSocket.
 * <p>
 * The class connects to a WebSocket server using the provided {@link StreamingResourceReference}, manages streaming
 * download requests (partial or full), and tracks the current transfer status. It supports chunked downloads and dynamically
 * adapts as new data becomes available during a streaming transfer.
 * </p>
 * <p>
 * Internally, it manages active chunk streams and a full download stream using {@link TrackablePipedInputStream}
 * and a {@link DelegatingInputStream}-based design. It also coordinates synchronization between asynchronous status
 * updates and blocking read requests.
 * </p>
 */
public class WebsocketDownload extends AbstractStreamingTransfer implements StreamingDownload {
    private static final Logger logger = LoggerFactory.getLogger(WebsocketDownload.class);
    private final WebsocketDownloadClient client;
    private final AtomicReference<TrackablePipedInputStream> activeChunkStream = new AtomicReference<>();
    private final AtomicReference<ChunkAwareInputStream> activeFullInputStream = new AtomicReference<>();
    private final AtomicReference<CompletableFuture<AtomicReference<StreamingResourceStatus>>> statusAwaitingFutureRef = new AtomicReference<>();

    /**
     * Constructs a new {@code WebsocketDownload} for the given resource reference.
     *
     * @param reference the resource reference pointing to the remote WebSocket endpoint
     * @throws IOException if the reference is invalid or other errors occur
     */
    public WebsocketDownload(StreamingResourceReference reference) throws IOException {
        setReference(reference);
        client = new WebsocketDownloadClient(reference.getUri());
        client.registerStatusListener(this::setCurrentStatus);
        client.registerCloseListener(this::onClientClose);
    }

    @Override
    public void setCurrentStatus(StreamingResourceStatus status) {
        logger.debug("Setting current status to {}", status);
        // this will notify potential downstream listeners
        super.setCurrentStatus(status);

        /*
         Yes, the following is complicated, but here's the reason. In edge cases, particularly 0-byte files, the following
         statuses will arrive in quick succession: INITIATED, IN_PROGRESS, COMPLETED -- all with a length of 0.
         Since the download will detect end-of-stream in the first two cases, it will immediately install a new
         future every time, to be notified about new statuses. That's why the future itself is wrapped in an AtomicReference.
         Now, in theory, between completion and evaluation of the future, a new message could arrive. This
         is why we may need to modify the result after it was signalled, and why the future contains a modifiable reference.
         Finally, the reading part will get the latest value and install a new future at the same time, during
         which it will need exclusive access to the top-level object. To ensure that any potential new statuses
         received will not get lost (because they could be "routed" to the old future), that's wrapped in a synchronized
         block there -- and here too, to avoid writing during that time.
         */
        synchronized (statusAwaitingFutureRef) {
            if (statusAwaitingFutureRef.get() != null) {
                var future = statusAwaitingFutureRef.get();
                if (!future.isDone()) {
                    future.complete(new AtomicReference<>(status));
                } else {
                    // join() will return immediately as the future is already completed, we're simply updating the value
                    future.join().set(status);
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    private void onClientClose() {
        logger.debug("Websocket client was closed");
        // if anything abnormal happened, there should have been exceptions propagating, but just in case
        var future = statusAwaitingFutureRef.get();
        if (future != null && !future.isDone()) {
            future.completeExceptionally(new IllegalStateException("Websocket client was closed"));
        }
    }

    /**
     * Returns a new InputStream to download a specific range (chunk) of the resource.
     * Fails if another chunk stream is still active and open.
     *
     * @param startOffset the starting byte offset (inclusive)
     * @param endOffset the ending byte offset (exclusive)
     * @return an input stream for the requested chunk
     * @throws IOException if the chunk range is invalid or the transfer cannot proceed
     */
    public TrackablePipedInputStream getChunkStream(long startOffset, long endOffset) throws IOException {
        StreamingResourceStatus status = getCurrentStatus();
        TrackablePipedInputStream pipedInputStream;
        synchronized (activeChunkStream) {
            if (activeChunkStream.get() != null && !activeChunkStream.get().isClosed()) {
                throw new IOException("There is already an active, non-closed chunk stream");
            }
            if (status == null) {
                throw new IOException("Resource unavailable");
            }
            StreamingResourceTransferStatus transferStatus = status.getTransferStatus();
            if (transferStatus == StreamingResourceTransferStatus.FAILED) {
                throw new IOException("Resource unavailable, server indicates resource status " + transferStatus);
            }
            if (startOffset < 0 || startOffset > endOffset) {
                throw new IOException("Illegal start/end offsets: [" + startOffset + ", " + endOffset + "]");
            }
            if (startOffset > status.getCurrentSize() || endOffset > status.getCurrentSize()) {
                throw new IOException("Data not available: [" + startOffset + ", " + endOffset + "], current available size is: " + status.getCurrentSize());
            }
            long bufferSize = Math.min(endOffset - startOffset, 128 * 1024);
            // special case for 0-byte requests
            if (bufferSize == 0) bufferSize = 1;

            pipedInputStream = new TrackablePipedInputStream((int) bufferSize);
            activeChunkStream.set(pipedInputStream);
        }

        PipedOutputStream pipedOutputStream = new PipedOutputStream(pipedInputStream);
        try {
            logger.debug("Requesting chunk stream for [{}, {}]", startOffset, endOffset);
            CompletableFuture<Long> consumed = client.requestChunkTransfer(startOffset, endOffset, pipedOutputStream);
            consumed.whenComplete((res, ex) -> {
                if (ex != null) {
                    logger.error("Error during chunk transfer", ex);
                    if (!pipedInputStream.isClosed()) {
                        try {
                            pipedInputStream.close();
                        } catch (IOException e) {
                            logger.error("Error closing piped input stream", e);
                        }
                    }
                } else {
                    logger.debug("Chunk stream transferred {} bytes", res);
                }
                try {
                    pipedOutputStream.close();
                } catch (IOException e) {
                    logger.error("Error closing PipedOutputStream", e);
                }
            });
            return pipedInputStream;
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    /**
     * Returns a single {@link InputStream} for the entire resource, reading sequentially as new chunks become available.
     * Fails if another input stream is already active.
     *
     * @return an input stream for the complete resource
     * @throws IOException if the stream cannot be initialized due to any reason
     */
    public InputStream getInputStream() throws IOException {
        synchronized (activeFullInputStream) {
            if (activeFullInputStream.get() != null && !activeFullInputStream.get().isClosed()) {
                throw new IOException("There is already an active, non-closed input stream");
            }
            activeFullInputStream.set(new ChunkAwareInputStream());
        }
        return activeFullInputStream.get();
    }

    /**
     * Internal input stream class that handles reading sequential chunks dynamically as the remote
     * transfer progresses. This stream delegates to multiple {@link TrackablePipedInputStream} instances
     * and tracks the total number of bytes read.
     * <p>
     * When a chunk is exhausted and the transfer is still in progress, it waits for status updates,
     * requesting subsequent chunks, until the entire file is available.
     * </p>
     */
    private class ChunkAwareInputStream extends DelegatingInputStream<TrackablePipedInputStream> {
        long bytesRead;

        public ChunkAwareInputStream() throws IOException {
            super();
        }

        /**
         * Returns the next available chunk stream based on the latest transfer status.
         * May block if the transfer is in progress but the data is not yet available.
         *
         * @return the next stream chunk, or {@code null} if EOF
         * @throws IOException if transfer fails or other failures occur
         */
        @Override
        protected TrackablePipedInputStream getNextDelegate() throws IOException {
            if (delegate != null && !delegate.isClosed()) {
                delegate.close();
            }
            // we register the future here before querying the status, to avoid potential race conditions
            // (status could be updated between querying it and noticing that we need to wait for a new one)
            synchronized (statusAwaitingFutureRef) {
                statusAwaitingFutureRef.set(new CompletableFuture<>());
            }
            StreamingResourceStatus status = getCurrentStatus();
            if (status == null) {
                throw new IOException("Resource unavailable");
            }
            logger.debug("Getting next chunk stream, bytesRead={}, last known status={}", bytesRead, status);
            return getStreamForStatus(status);
        }

        private TrackablePipedInputStream getStreamForStatus(StreamingResourceStatus status) throws IOException {
            if (status.getTransferStatus() == StreamingResourceTransferStatus.FAILED) {
                throw new IOException("Resource unavailable, server indicates resource status " + StreamingResourceTransferStatus.FAILED);
            }
            if (status.getCurrentSize() > bytesRead) {
                return getChunkStream(bytesRead, status.getCurrentSize());
            }
            if (status.getTransferStatus() == StreamingResourceTransferStatus.COMPLETED) {
                logger.debug("Transfer status is {} and all bytes were received, signaling end of data", status.getTransferStatus());
                return null; // normal EOF, no more streams required
            }
            try {
                logger.debug("Current chunk stream is exhausted, waiting for new status update (last known status={})", status);
                // See the comment in setCurrentStatus for a detailed explanation of the flow, and the rationale.
                StreamingResourceStatus newStatus;
                // Wait for future to complete.
                AtomicReference<StreamingResourceStatus> newStatusReference = statusAwaitingFutureRef.get().get();
                // synchronized in order to briefly block possible new updates
                synchronized (statusAwaitingFutureRef) {
                    newStatus = newStatusReference.get();
                    // install a new future to be completed next.
                    statusAwaitingFutureRef.set(new CompletableFuture<>());
                }
                logger.debug("New status received: {}", newStatus);
                return getStreamForStatus(newStatus);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int read = super.read(b, off, len);
            if (read != -1) {
                bytesRead += read;
            }
            return read;
        }
    }

    @Override
    protected void onStatusCallbackFailed(Consumer<StreamingResourceStatus> callback, Exception exception) {
        logger.error("Error while invoking status callback {}", callback, exception);
    }
}
