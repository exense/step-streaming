package step.streaming.common;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A StreamingResourceUploadContext is a temporally limited context during which uploads
 * may happen, for instance a keyword call in Step. A context's lifecycle MUST be registered using
 * {@link StreamingResourceUploadContexts#registerContext(StreamingResourceUploadContext)}
 * on instantiation, and unregistered using {@link StreamingResourceUploadContexts#unregisterContext(StreamingResourceUploadContext)}
 *
 * A context is identified by a unique auto-generated ID, which may be passed around to identify it e.g. when an upload happens etc.
 * A context can carry arbitrary attributes (exposed as a read/write Map)
 *
 * @see StreamingResourceUploadContexts
 */
public class StreamingResourceUploadContext {
    // Meant to be used for instance in attribute maps, as URL parameter name etc. to identify a context
    public static final String PARAMETER_NAME = "streamingUploadContextId";

    public final String contextId;

    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public StreamingResourceUploadContext() {
        this.contextId = UUID.randomUUID().toString();
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    @Override
    public String toString() {
        return "StreamingResourceUploadContext{" +
                "contextId='" + contextId + '\'' +
                ", attributes=" + attributes +
                '}';
    }
}
