package step.streaming.server.test;

import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.server.StreamingResourcesCatalogBackend;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class InMemoryCatalogBackend implements StreamingResourcesCatalogBackend {

    private static class CatalogEntry {
        final String name;
        final String type;
        StreamingResourceStatus status;

        CatalogEntry(String name, String type, StreamingResourceStatus status) {
            this.name = name;
            this.type = type;
            this.status = status;
        }
    }

    private final Map<String, CatalogEntry> catalog = new ConcurrentHashMap<>();

    @Override
    public String createResourceReference(StreamingResourceMetadata metadata) {
        String resourceId = UUID.randomUUID().toString();
        catalog.put(resourceId, new CatalogEntry(
                metadata.getFilename(),
                metadata.getMimeType(),
                new StreamingResourceStatus(StreamingResourceTransferStatus.INITIATED, 0L)
        ));
        return resourceId;
    }

    @Override
    public void updateStatus(String resourceId, StreamingResourceStatus status) {
        CatalogEntry entry = catalog.get(resourceId);
        if (entry == null) {
            throw new IllegalArgumentException("Resource ID not found: " + resourceId);
        }
        entry.status = status;
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
}
