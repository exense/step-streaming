package step.streaming.websocket.client.upload;

import jakarta.websocket.ContainerProvider;
import jakarta.websocket.WebSocketContainer;
import org.eclipse.jetty.util.component.LifeCycle;
import org.eclipse.jetty.websocket.core.client.WebSocketCoreClient;
import org.eclipse.jetty.websocket.jakarta.client.internal.JakartaWebSocketClientContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.client.upload.StreamingUploadProvider;
import step.streaming.client.upload.impl.AbstractStreamingUploadProvider;
import step.streaming.common.QuotaExceededException;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.data.EndOfInputSignal;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
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
    private static final Logger logger = LoggerFactory.getLogger(WebsocketUploadProvider.class);

    /**
     * Creates a new {@code WebsocketUploadProvider} with a custom pool size for asynchronous uploads.
     *
     * @param container       the WebSocketContainer to use for constructing clients
     * @param executorService the ExecutorService to use for concurrent uploads
     * @param endpointUri     the WebSocket endpoint URI to which uploads should be directed
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
        WebSocketContainer container = ContainerProvider.getWebSocketContainer();
        initializeContainer(container);
        return container;
    }

    @Override
    protected WebsocketUploadSession startLiveFileUpload(InputStream sourceInputStream, StreamingResourceMetadata metadata, EndOfInputSignal endOfInputSignal) throws QuotaExceededException, IOException {
        WebsocketUploadSession upload = new WebsocketUploadSession(Objects.requireNonNull(metadata), endOfInputSignal);
        WebsocketUploadClient client = new WebsocketUploadClient(container, endpointUri, upload);
        executorService.submit(() -> client.performUpload(sourceInputStream));
        return upload;
    }

    private static void initializeContainer(WebSocketContainer container) {
        // we need to pre-start the container components so they're ready to use, otherwise we can get weird exceptions
        // like "java.lang.IllegalStateException: WebSocketCoreClient is not started" when the components try
        // lazy initialization concurrently from multiple threads.
        if (container instanceof JakartaWebSocketClientContainer) {
            try {
                //  I *hate* the way that this stuff is implemented. There are no meaningful methods in the interfaces,
                //  and even the implementations hide the important stuff away by making the methods protected.
                JakartaWebSocketClientContainer clientContainer = (JakartaWebSocketClientContainer) container;
                clientContainer.start();
                Method getWebSocketCoreClient = clientContainer.getClass().getDeclaredMethod("getWebSocketCoreClient");
                getWebSocketCoreClient.setAccessible(true);
                WebSocketCoreClient coreClient = (WebSocketCoreClient) getWebSocketCoreClient.invoke(clientContainer);
                coreClient.start();
                int waits = 10;
                while (!coreClient.isStarted() && waits-- > 0) {
                    logger.debug("Waiting for Websocket core client to start (iterations left: {})", waits);
                    Thread.sleep(100);
                }
            } catch (Exception e) {
                logger.error("Error initializing Websocket Container", e);
            }
        }
    }
}
