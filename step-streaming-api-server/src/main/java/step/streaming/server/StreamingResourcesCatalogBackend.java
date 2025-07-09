package step.streaming.server;

import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceUploadContext;

/** Catalog backend used on the server side.
 * The catalog keeps track of resource metadata.
 */
public interface StreamingResourcesCatalogBackend {

    /**
     * Creates a new resource and registers its metadata.
     *
     * @param metadata resource metadata
     * @param uploadContext upload context, potentially null depending on configuration
     * @return a unique internal resource ID
     */
    String createResource(StreamingResourceMetadata metadata, StreamingResourceUploadContext uploadContext);

    /**
     * Updates the current transfer status and size of the resource.
     *
     * @param resourceId internal resource identifier
     * @param status     the updated status object
     */
    void updateStatus(String resourceId, StreamingResourceStatus status);

    /**
     * Retrieves the current status of the resource.
     *
     * @param resourceId internal resource identifier
     * @return the status metadata
     */
    StreamingResourceStatus getStatus(String resourceId);
}
