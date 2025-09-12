package step.streaming.websocket.client.upload;

import jakarta.websocket.ContainerProvider;
import jakarta.websocket.WebSocketContainer;
import step.streaming.client.upload.StreamingUploadProvider;
import step.streaming.client.upload.impl.AbstractStreamingUploadProvider;
import step.streaming.common.QuotaExceededException;
import step.streaming.common.StreamingResourceMetadata;
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
    private final WebSocketContainer container;

    /**
     * Creates a new {@code WebsocketUploadProvider} with a custom pool size for asynchronous uploads.
     *
     * @param endpointUri     the WebSocket endpoint URI to which uploads should be directed
     * @param executorService the ExecutorService to use for concurrent uploads
     */
    public WebsocketUploadProvider(WebSocketContainer container, ExecutorService executorService, URI endpointUri) {
        super(executorService);
        this.container = container;
        this.endpointUri = Objects.requireNonNull(endpointUri);
    }

    // Helper constructor for cases where the provider is used in a separate class loader, and the outer code
    // does not know about the WebSocketContainer class, but simply treats it as a generic object
    public WebsocketUploadProvider(Object container, ExecutorService executorService, URI endpointUri) {
        this((WebSocketContainer) container, executorService, endpointUri);
    }

    // Helper method for abovementioned use case: instantiate a new container and return as Object.
    public static Object instantiateWebSocketContainer() {
        return ContainerProvider.getWebSocketContainer();
    }

    @Override
    protected WebsocketUploadSession startLiveFileUpload(InputStream sourceInputStream, StreamingResourceMetadata metadata, EndOfInputSignal endOfInputSignal) throws QuotaExceededException, IOException {
        WebsocketUploadSession upload = new WebsocketUploadSession(Objects.requireNonNull(metadata), endOfInputSignal);
        WebsocketUploadClient client = new WebsocketUploadClient(container, endpointUri, upload);
        executorService.submit(() -> client.performUpload(sourceInputStream));
        return upload;
    }
}
