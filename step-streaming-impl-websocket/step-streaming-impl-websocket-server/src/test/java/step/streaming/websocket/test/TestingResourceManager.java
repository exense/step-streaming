package step.streaming.websocket.test;

import step.streaming.common.StreamingResourceReference;
import step.streaming.common.StreamingResourceUploadContexts;
import step.streaming.server.DefaultStreamingResourceManager;
import step.streaming.server.StreamingResourcesCatalogBackend;
import step.streaming.server.StreamingResourcesStorageBackend;
import step.streaming.util.ExceptionsUtil;
import step.streaming.util.ThrowingConsumer;

import java.io.IOException;
import java.util.function.Function;

public class TestingResourceManager extends DefaultStreamingResourceManager {
    public boolean uploadContextRequired = false;
    public ThrowingConsumer<Long> sizeChecker = null;

    public TestingResourceManager(StreamingResourcesCatalogBackend catalog, StreamingResourcesStorageBackend storage, Function<String, StreamingResourceReference> referenceProducerFunction, StreamingResourceUploadContexts uploadContexts) {
        super(catalog, storage, referenceProducerFunction, uploadContexts);
    }

    @Override
    public boolean isUploadContextRequired() {
        return uploadContextRequired;
    }

    @Override
    protected void onSizeChanged(String resourceId, long currentSize) throws IOException {
        if (sizeChecker != null) {
            System.err.println("onSizeChanged: " + resourceId + " -> " + currentSize);
            try {
                sizeChecker.accept(currentSize);
            } catch (Exception e) {
                throw ExceptionsUtil.as(e, IOException.class);
            }
        }
    }
}
