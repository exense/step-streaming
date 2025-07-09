package step.streaming.common;

public interface StreamingResourceUploadContextListener {
    void onResourceCreated(String resourceId, StreamingResourceMetadata metadata);
    void onResourceStatusChanged(String resourceId, StreamingResourceStatus status);
}
