package step.streaming.websocket.client.upload;

import step.streaming.client.upload.StreamingUploadProvider;
import step.streaming.client.upload.impl.AbstractStreamingUploadProvider;
import step.streaming.common.QuotaExceededException;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.data.LimitedBufferInputStream;
import step.streaming.data.EndOfInputSignal;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * A {@link StreamingUploadProvider} implementation that uses WebSockets for transferring files.
 * <p>
 * Uploads are managed asynchronously using a thread pool.
 * </p>
 */
public class WebsocketUploadProvider extends AbstractStreamingUploadProvider {
    protected final URI endpointUri;

    /**
     * Creates a new {@code WebsocketUploadProvider} with a custom pool size for asynchronous uploads.
     *
     * @param endpointUri     the WebSocket endpoint URI to which uploads should be directed
     * @param executorService the ExecutorService to use for concurrent uploads
     */
    public WebsocketUploadProvider(ExecutorService executorService, URI endpointUri) {
        super(executorService);
        this.endpointUri = Objects.requireNonNull(endpointUri);
    }

    @Override
    protected WebsocketUploadSession startLiveFileUpload(InputStream sourceInputStream, StreamingResourceMetadata metadata, EndOfInputSignal endOfInputSignal) throws QuotaExceededException, IOException {
        WebsocketUploadSession upload = new WebsocketUploadSession(Objects.requireNonNull(metadata), endOfInputSignal);
        WebsocketUploadClient client = new WebsocketUploadClient(endpointUri, upload);
        executorService.execute(() -> client.performUpload(sourceInputStream));
        return upload;
    }
}
