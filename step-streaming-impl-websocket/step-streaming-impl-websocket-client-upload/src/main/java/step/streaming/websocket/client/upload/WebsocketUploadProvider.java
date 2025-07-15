package step.streaming.websocket.client.upload;

import step.streaming.client.upload.StreamingUpload;
import step.streaming.client.upload.StreamingUploadProvider;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.data.LiveFileInputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import step.streaming.data.EndOfInputSignal;
import step.streaming.data.UTF8TranscodingTextInputStream;

/**
 * A {@link StreamingUploadProvider} implementation that uses WebSockets for transferring files.
 * <p>
 * Uploads are managed asynchronously using a thread pool.
 * </p>
 */
public class WebsocketUploadProvider implements StreamingUploadProvider {
    public static final long DEFAULT_FILE_POLL_INTERVAL_MS = 10;
    public static final int DEFAULT_CONCURRENT_UPLOAD_POOL_SIZE = 100;
    protected final URI endpointUri;
    protected final ExecutorService executorService;

    /**
     * Creates a new {@code WebsocketUploadProvider} with the default pool size for asynchronous uploads.
     *
     * @param endpointUri the WebSocket endpoint URI to which uploads should be directed
     */
    public WebsocketUploadProvider(URI endpointUri) {
        this(endpointUri, DEFAULT_CONCURRENT_UPLOAD_POOL_SIZE);
    }

    /**
     * Creates a new {@code WebsocketUploadProvider} with a custom pool size for asynchronous uploads.
     *
     * @param endpointUri the WebSocket endpoint URI to which uploads should be directed
     * @param concurrentUploadPoolSize the number of threads used for concurrent uploads
     */
    public WebsocketUploadProvider(URI endpointUri, int concurrentUploadPoolSize) {
        this.endpointUri = Objects.requireNonNull(endpointUri);
        this.executorService = Executors.newFixedThreadPool(concurrentUploadPoolSize);
    }

    @Override
    public StreamingUpload startLiveBinaryFileUpload(File fileToStream, StreamingResourceMetadata metadata) throws IOException {
        return startLiveFileUpload(fileToStream, metadata, null);
    }

    @Override
    public StreamingUpload startLiveTextFileUpload(File textFile, StreamingResourceMetadata metadata, Charset charset) throws IOException {
        return startLiveFileUpload(textFile, metadata, charset);
    }

    private StreamingUpload startLiveFileUpload(File fileToStream, StreamingResourceMetadata metadata, Charset convertFromCharset) throws IOException {
        Objects.requireNonNull(fileToStream);
        EndOfInputSignal endOfInputSignal = new EndOfInputSignal();
        LiveFileInputStream liveInputStream = new LiveFileInputStream(fileToStream, endOfInputSignal, DEFAULT_FILE_POLL_INTERVAL_MS);
        WebsocketUpload upload = new WebsocketUpload(Objects.requireNonNull(metadata), endOfInputSignal);
        WebsocketUploadClient client = new WebsocketUploadClient(endpointUri, upload);
        InputStream uploadInputStream = convertFromCharset == null ? liveInputStream : new UTF8TranscodingTextInputStream(liveInputStream, convertFromCharset);
        executorService.execute(() -> client.performUpload(uploadInputStream));
        return upload;
    }


}
