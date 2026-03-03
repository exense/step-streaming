package step.streaming.server.test;

import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.common.StreamingResourceUploadContext;
import step.streaming.server.StreamingResourceStatusUpdate;
import step.streaming.server.StreamingResourcesCatalogBackend;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryCatalogBackend implements StreamingResourcesCatalogBackend {

    public static class CatalogEntry {
        public final String name;
        public final String type;
        StreamingResourceStatus status;

        CatalogEntry(String name, String type, StreamingResourceStatus status) {
            this.name = name;
            this.type = type;
            this.status = status;
        }
    }

    public final Map<String, CatalogEntry> catalog = new ConcurrentHashMap<>();

    @Override
    public String createResource(StreamingResourceMetadata metadata, StreamingResourceUploadContext uploadContext) {
        String resourceId = UUID.randomUUID().toString();
        catalog.put(resourceId, new CatalogEntry(
            metadata.getFilename(),
            metadata.getMimeType(),
            new StreamingResourceStatus(StreamingResourceTransferStatus.INITIATED, 0L, metadata.getSupportsLineAccess() ? 0L : null)
        ));
        return resourceId;
    }

    @Override
    public StreamingResourceStatus updateStatus(String resourceId, StreamingResourceStatusUpdate statusUpdate) {
        CatalogEntry entry = catalog.get(resourceId);
        if (entry == null) {
            throw new IllegalArgumentException("Resource ID not found: " + resourceId);
        }
        entry.status = statusUpdate.applyTo(entry.status);
        return entry.status;
    }

    @Override
    public StreamingResourceStatus getStatus(String resourceId) {
        CatalogEntry entry = catalog.get(resourceId);
        return entry != null ? entry.status : null;
    }

    // Optional: expose test utility method
    public String getName(String resourceId) {
        CatalogEntry entry = catalog.get(resourceId);
        return entry != null ? entry.name : null;
    }

    @Override
    public void delete(String resourceId) {
        catalog.remove(resourceId);
    }
}
