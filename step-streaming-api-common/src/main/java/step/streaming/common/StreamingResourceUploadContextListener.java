package step.streaming.common;

/**
 * Listener interface for observing streaming resource upload context events.
 * <p>
 * Implementations of this interface can be registered to receive notifications when:
 * <ul>
 *   <li>a new streaming resource is created</li>
 *   <li>the status of an existing streaming resource changes</li>
 *   <li>an attempt to create a streaming resource failed</li>
 * </ul>
 * These callbacks are typically used for tracking upload progress, reacting to completion, or handling errors.
 */
public interface StreamingResourceUploadContextListener {

    /**
     * Called when a new streaming resource has been created within the context.
     *
     * @param resourceId the unique identifier of the newly created resource
     * @param metadata   the metadata associated with the resource
     */
    void onResourceCreated(String resourceId, StreamingResourceMetadata metadata);

    /**
     * Called when the status of a streaming resource has changed.
     *
     * @param resourceId the unique identifier of the resource
     * @param status     the new status of the resource
     */
    void onResourceStatusChanged(String resourceId, StreamingResourceStatus status);

    /**
     * Called when a streaming upload was requested by a client, but was refused by the server, for instance
     * due to quota restrictions.
     * @param metadata the resource metadata associated with the rejected upload request
     * @param reasonPhrase a human-readable explanation for refusing the upload
     */
    void onResourceCreationRefused(StreamingResourceMetadata metadata, String reasonPhrase);
}
