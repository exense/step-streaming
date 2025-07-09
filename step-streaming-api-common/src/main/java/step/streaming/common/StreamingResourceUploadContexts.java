package step.streaming.common;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

public class StreamingResourceUploadContexts {
    private final Map<String, StreamingResourceUploadContext> activeContexts = new ConcurrentHashMap<>();
    private final Map<String, List<StreamingResourceUploadContextListener>> listeners = new ConcurrentHashMap<>();
    private final List<Consumer<String>> contextUnregisteredCallbacks = new CopyOnWriteArrayList<>();

    public void registerContextUnregisteredCallback(Consumer<String> contextUnregisteredCallback) {
        contextUnregisteredCallbacks.add(contextUnregisteredCallback);
    }

    public void registerContext(StreamingResourceUploadContext context) {
        activeContexts.put(context.contextId, context);
    }

    public StreamingResourceUploadContext getContext(String contextId) {
        return activeContexts.get(contextId);
    }

    public void unregisterContext(String contextId) {
        listeners.remove(contextId);
        activeContexts.remove(contextId);
        contextUnregisteredCallbacks.forEach(c -> c.accept(contextId));
    }

    public void registerListener(String contextId, StreamingResourceUploadContextListener listener) {
        if (!activeContexts.containsKey(contextId)) {
            throw new IllegalArgumentException("No active context with id " + contextId);
        }
        listeners.computeIfAbsent(contextId, k -> new CopyOnWriteArrayList<>()).add(listener);
    }

    private List<StreamingResourceUploadContextListener> getListeners(String uploadContextId) {
        if (uploadContextId != null) {
            List<StreamingResourceUploadContextListener> list = listeners.get(uploadContextId);
            if (list != null) {
                return list;
            }
        }
        // fallback to empty list
        return List.of();
    }

    public void onResourceCreated(String uploadContextId, String resourceId, StreamingResourceMetadata metadata) {
        for (StreamingResourceUploadContextListener listener : getListeners(uploadContextId)) {
            listener.onResourceCreated(resourceId, metadata);
        }
    }

    public void onResourceStatusChanged(String uploadContextId, String resourceId, StreamingResourceStatus status) {
        for (StreamingResourceUploadContextListener listener : getListeners(uploadContextId)) {
            listener.onResourceStatusChanged(resourceId, status);
        }
    }

}
