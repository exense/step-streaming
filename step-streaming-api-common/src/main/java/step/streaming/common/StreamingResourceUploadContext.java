package step.streaming.common;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class StreamingResourceUploadContext {
    // Meant to be used for instance in Services/Endpoints/other parameters to identify a context
    public static final String PARAMETER_NAME = "streamingUploadContextId";

    public final String contextId;

    private final Map<String, Object> attributes = new ConcurrentHashMap<>();

    public StreamingResourceUploadContext(String contextId) {
        this.contextId = Objects.requireNonNull(contextId);
    }

    public StreamingResourceUploadContext() {
        this(UUID.randomUUID().toString());
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
