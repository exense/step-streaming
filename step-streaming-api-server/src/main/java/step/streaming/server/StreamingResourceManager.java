package step.streaming.server;

import step.streaming.common.QuotaExceededException;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceReference;
import step.streaming.common.StreamingResourceStatus;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

/**
 * Manager coordinating server-side operations related to streaming resources.
 * TODO: document.
 */
public interface StreamingResourceManager {

    default boolean isUploadContextRequired() {
        return false;
    }

    ExecutorService getUploadsThreadPool();

    String registerNewResource(StreamingResourceMetadata metadata, String uploadContextId) throws QuotaExceededException, IOException;

    void deleteResource(String resourceId) throws IOException;

    long writeChunk(String resourceId, InputStream input, boolean isFinal) throws IOException;

    StreamingResourceStatus markCompleted(String resourceId);

    void markFailed(String resourceId);

    StreamingResourceStatus getStatus(String resourceId);

    InputStream openStream(String resourceId, long start, long end) throws IOException;

    void registerStatusListener(String resourceId, Consumer<StreamingResourceStatus> listener);

    void unregisterStatusListener(String resourceId, Consumer<StreamingResourceStatus> listener);

    StreamingResourceReference getReferenceFor(String resourceId);

    List<Long> getLinebreakPositions(String resourceId, long startingLinebreakIndex, long count) throws IOException;

    List<String> getLines(String resourceId, long startingLineIndex, long count) throws IOException;
}
