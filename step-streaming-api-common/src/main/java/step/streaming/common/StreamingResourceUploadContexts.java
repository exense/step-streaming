package step.streaming.common;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Central registry and event hub for managing active {@link StreamingResourceUploadContext}s
 * and their associated listeners and resources.
 *
 * <p>
 * This class provides functionality to:
 * <ul>
 *   <li>Register and unregister active upload contexts</li>
 *   <li>Register listeners for per-context upload events</li>
 *   <li>Dispatch resource creation and status change notifications to listeners</li>
 *   <li>Track resource-to-context mappings</li>
 * </ul>
 * </p>
 *
 * <p>
 * Listener registration is validated against active context IDs to avoid dangling registrations.
 * Upon context unregistration, all listeners and resource mappings associated with the context
 * are also removed.
 * </p>
 *
 * @see StreamingResourceUploadContext
 * @see StreamingResourceUploadContextListener
 */
public class StreamingResourceUploadContexts {
    /**
     * Active upload contexts mapped by their own unique context ID.
     */
    private final Map<String, StreamingResourceUploadContext> activeContexts = new ConcurrentHashMap<>();

    /**
     * Per-context listener lists for upload-related events.
     */
    private final Map<String, List<StreamingResourceUploadContextListener>> listeners = new ConcurrentHashMap<>();

    private final Map<String, String> resourceIdToContextIdMap = new ConcurrentHashMap<>();

    /**
     * Registers/activates a new {@link StreamingResourceUploadContext}.
     *
     * @param context the context to register
     */
    public void registerContext(StreamingResourceUploadContext context) {
        activeContexts.put(context.contextId, context);
    }

    /**
     * Retrieves an active upload context by its context ID.
     *
     * @param contextId the ID of the context to retrieve
     * @return the corresponding {@link StreamingResourceUploadContext}, or null if not found
     */
    public StreamingResourceUploadContext getContext(String contextId) {
        return activeContexts.get(contextId);
    }

    /**
     * Unregisters/deactivates an existing upload context, and cleans up related data and listeners
     *
     * @param context the context to unregister
     */
    public void unregisterContext(StreamingResourceUploadContext context) {
        listeners.remove(context.contextId);
        resourceIdToContextIdMap.entrySet().removeIf(entry -> context.contextId.equals(entry.getValue()));
        activeContexts.remove(context.contextId);
    }

    /**
     * Registers a listener for a specific upload context ID.
     *
     * @param contextId the ID of the upload context
     * @param listener  the listener to associate with the context
     * @throws IllegalArgumentException if the context ID is not active
     */
    public void registerListener(String contextId, StreamingResourceUploadContextListener listener) {
        if (!activeContexts.containsKey(contextId)) {
            throw new IllegalArgumentException("No active context with id " + contextId);
        }
        listeners.computeIfAbsent(contextId, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    /**
     * Returns the list of listeners associated with a given upload context ID, or an empty list if none present.
     *
     * @param uploadContextId the ID of the upload context
     * @return a list of registered listeners (possibly empty, never null)
     */
    private List<StreamingResourceUploadContextListener> getListeners(String uploadContextId) {
        if (uploadContextId != null) {
            List<StreamingResourceUploadContextListener> list = listeners.get(uploadContextId);
            if (list != null) {
                return list;
            }
        }
        return List.of(); // fallback to empty list
    }

    /**
     * Associates the resourceId with the given contextId, and notifies all registered
     * listeners that a new resource has been created within the given context.
     *
     * @param uploadContextId the upload context ID
     * @param resourceId      the ID of the newly created resource
     * @param metadata        associated metadata for the resource
     */
    public void onResourceCreated(String uploadContextId, String resourceId, StreamingResourceMetadata metadata) {
        resourceIdToContextIdMap.put(Objects.requireNonNull(resourceId), Objects.requireNonNull(uploadContextId));
        for (StreamingResourceUploadContextListener listener : getListeners(uploadContextId)) {
            try {
                listener.onResourceCreated(resourceId, metadata);
            } catch (Exception ignored) {} // we don't have access to a logger here, but these methods must not throw exceptions anyway; this is just a safeguard.
        }
    }

    public String getContextIdForResourceId(String resourceId) {
        return resourceIdToContextIdMap.get(resourceId);
    }


    /**
     * Notifies all registered listeners that the status of a resource has changed within the given context.
     *
     * @param uploadContextId the upload context ID
     * @param resourceId      the ID of the affected resource
     * @param status          the new status of the resource
     */
    public void onResourceStatusChanged(String uploadContextId, String resourceId, StreamingResourceStatus status) {
        for (StreamingResourceUploadContextListener listener : getListeners(uploadContextId)) {
            try {
                listener.onResourceStatusChanged(resourceId, status);
            } catch (Exception ignored) {} // we don't have access to a logger here, but these methods must not throw exceptions anyway; this is just a safeguard.
        }
    }

    /**
     * Notifies all registered listeners that an attempt to create a resource within the given context was refused.
     * This might happen for instance if there are quota restrictions on the number of permitted resources per context.
     * @param uploadContextId the upload context ID
     * @param metadata metadata of the attempted upload
     * @param reasonPhrase human-readable reason for refusing the upload
     */
    public void onResourceCreationRefused(String uploadContextId, StreamingResourceMetadata metadata, String reasonPhrase) {
        for (StreamingResourceUploadContextListener listener : getListeners(uploadContextId)) {
            try {
                listener.onResourceCreationRefused(metadata, reasonPhrase);
            } catch (Exception ignored) {} // we don't have access to a logger here, but these methods must not throw exceptions anyway; this is just a safeguard.
        }
    }

}
