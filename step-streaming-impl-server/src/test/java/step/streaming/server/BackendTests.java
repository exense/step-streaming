package step.streaming.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import step.streaming.server.test.InMemoryCatalogBackend;
import step.streaming.server.test.TestingStorageBackend;

public class BackendTests {
    TestingStorageBackend storage;

    @Before
    public void setup() {
        storage = new TestingStorageBackend(FilesystemStreamingResourcesStorageBackend.DEFAULT_NOTIFY_INTERVAL_MILLIS, false);
    }

    @After
    public void cleanup() {
        storage.cleanup();
    }

    @Test
    public void testUploadWithTwoDownloads() throws Exception {
        StreamingResourcesCatalogBackend catalog = new InMemoryCatalogBackend();
        StreamingResourceManager manager = new DefaultStreamingResourceManager(catalog, storage,
                s -> null,
                null
        );
        storage.cleanup();
    }
}
