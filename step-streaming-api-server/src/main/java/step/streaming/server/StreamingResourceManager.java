package step.streaming.server;

import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceStatus;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

/** Manager coordinating server-side operations related to streaming resources.
 * TODO: document.
 */
public interface StreamingResourceManager {

    String registerNewResource(StreamingResourceMetadata metadata, String uploadContextId);

    long writeChunk(String resourceId, InputStream input) throws IOException;

    void markCompleted(String resourceId);

    void markFailed(String resourceId);

    StreamingResourceStatus getStatus(String resourceId);

    InputStream openStream(String resourceId, long start, long end) throws IOException;

    void registerStatusListener(String resourceId, Consumer<StreamingResourceStatus> listener);

    void unregisterStatusListener(String resourceId, Consumer<StreamingResourceStatus> listener);

    StreamingResourceReferenceMapper getReferenceMapper();
}
