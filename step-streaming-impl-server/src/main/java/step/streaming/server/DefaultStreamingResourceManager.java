package step.streaming.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DefaultStreamingResourceManager implements StreamingResourceManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultStreamingResourceManager.class);

    private final StreamingResourcesCatalogBackend catalog;
    private final StreamingResourcesStorageBackend storage;
    private final StreamingResourceUploadContexts uploadContexts;
    private final Function<String, StreamingResourceReference> referenceProducerFunction;

    // Listeners interested in a single resource (e.g. download clients)
    private final Map<String, CopyOnWriteArrayList<Consumer<StreamingResourceStatus>>> statusListeners = new ConcurrentHashMap<>();

    public DefaultStreamingResourceManager(StreamingResourcesCatalogBackend catalog,
                                           StreamingResourcesStorageBackend storage,
                                           Function<String, StreamingResourceReference> referenceProducerFunction,
                                           StreamingResourceUploadContexts uploadContexts
    ) {
        this.catalog = Objects.requireNonNull(catalog);
        this.storage = Objects.requireNonNull(storage);
        this.referenceProducerFunction = Objects.requireNonNull(referenceProducerFunction);
        this.uploadContexts = uploadContexts;
    }

    @Override
    public StreamingResourceReference getReferenceFor(String resourceId) {
        return referenceProducerFunction.apply(resourceId);
    }

    @Override
    public String registerNewResource(StreamingResourceMetadata metadata, String uploadContextId) throws IOException{
        StreamingResourceUploadContext uploadContext = (uploadContextId != null && uploadContexts != null) ? uploadContexts.getContext(uploadContextId) : null;
        String resourceId = catalog.createResource(metadata, uploadContext);
        logger.debug("Created new streaming resource: {}, context={}", resourceId, uploadContextId);

        try {
            storage.prepareForWrite(resourceId);
        } catch (IOException e) {
            markFailed(resourceId);
            logger.error("Storage backend failed to prepare resource {} for writing", resourceId, e);
            throw e;
        }
        if (uploadContext != null) {
            uploadContexts.onResourceCreated(uploadContextId, resourceId, metadata);
        }
        StreamingResourceStatus initStatus = new StreamingResourceStatus(StreamingResourceTransferStatus.INITIATED, 0L);
        catalog.updateStatus(resourceId, initStatus);
        emitStatus(resourceId, initStatus);

        return resourceId;
    }

    @Override
    public long writeChunk(String resourceId, InputStream input) throws IOException {
        try {
            storage.writeChunk(resourceId, input, updatedSize -> {
                StreamingResourceStatus status = new StreamingResourceStatus(
                        StreamingResourceTransferStatus.IN_PROGRESS, updatedSize
                );
                logger.debug("Updated streaming resource: {}, status={}", resourceId, status);
                catalog.updateStatus(resourceId, status);
                emitStatus(resourceId, status);
            });

            long currentSize = storage.getCurrentSize(resourceId);
            logger.debug("Delegated chunk write for {} (current size: {})", resourceId, currentSize);
            return currentSize;
        } catch (IOException e) {
            logger.warn("IOException during writeChunk for {} — marking FAILED", resourceId, e);
            markFailed(resourceId);
            throw e;
        }
    }

    @Override
    public void markCompleted(String resourceId) {
        try {
            long finalSize = storage.getCurrentSize(resourceId);
            StreamingResourceStatus status = new StreamingResourceStatus(StreamingResourceTransferStatus.COMPLETED, finalSize);
            logger.debug("Resource marked COMPLETED: {} (size: {})", resourceId, finalSize);
            catalog.updateStatus(resourceId, status);
            emitStatus(resourceId, status);
        } catch (IOException e) {
            logger.warn("IOException during markCompleted for {}, marking as failed instead", resourceId, e);
            markFailed(resourceId);
        }
    }

    @Override
    public void markFailed(String resourceId) {
        storage.handleFailedUpload(resourceId);
        StreamingResourceStatus status = new StreamingResourceStatus(
                StreamingResourceTransferStatus.FAILED, null
        );
        catalog.updateStatus(resourceId, status);
        logger.warn("Resource marked FAILED: {}", resourceId);
        emitStatus(resourceId, status);
    }

    @Override
    public StreamingResourceStatus getStatus(String resourceId) {
        return catalog.getStatus(resourceId);
    }

    @Override
    public InputStream openStream(String resourceId, long start, long end) throws IOException {
        logger.debug("Opening stream for {}, chunk [{}, {}]", resourceId, start, end);
        return storage.openReadStream(resourceId, start, end);
    }

    @Override
    public void registerStatusListener(String resourceId, Consumer<StreamingResourceStatus> listener) {
        statusListeners
                .computeIfAbsent(resourceId, k -> new CopyOnWriteArrayList<>())
                .add(listener);

        logger.debug("Registered status listener for {}", resourceId);

        StreamingResourceStatus currentStatus = catalog.getStatus(resourceId);
        if (currentStatus != null) {
            listener.accept(currentStatus);
        }
    }

    @Override
    public void unregisterStatusListener(String resourceId, Consumer<StreamingResourceStatus> listener) {
        var listeners = statusListeners.get(resourceId);
        if (listeners != null) {
            if (listeners.remove(listener)) {
                logger.debug("Unregistered status listener for {}", resourceId);
            }
            if (listeners.isEmpty()) {
                statusListeners.remove(resourceId);
            }
        }
    }

    private void emitStatus(String resourceId, StreamingResourceStatus status) {
        var listeners = statusListeners.get(resourceId);
        if (listeners != null) {
            logger.debug("Emitting status update to {} listener(s) for {}: {}", listeners.size(), resourceId, status);
            for (var listener : listeners) {
                try {
                    listener.accept(status);
                } catch (Exception e) {
                    logger.error("Status listener unexpectedly threw exception {} on resource {}, status {}", listener, resourceId, status, e);
                }
            }
        }
        if (uploadContexts != null) {
            String uploadContextId = uploadContexts.getContextIdForResourceId(resourceId);
            if (uploadContextId != null) {
                uploadContexts.onResourceStatusChanged(uploadContextId, resourceId, status);
            }
        }
    }
}
