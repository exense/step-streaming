package step.streaming.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.server.test.FailingInputStream;
import step.streaming.server.test.InMemoryCatalogBackend;
import step.streaming.server.test.TestingStorageBackend;

import java.io.*;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

public class DefaultStreamingResourceManagerTest {

    private TestingStorageBackend storageBackend;
    private InMemoryCatalogBackend catalogBackend;
    private DefaultStreamingResourceManager manager;

    @Before
    public void setUp() throws IOException {
        storageBackend = new TestingStorageBackend(StreamingResourcesStorageBackend.DEFAULT_NOTIFY_INTERVAL_MILLIS, false);
        catalogBackend = new InMemoryCatalogBackend();
        manager = new DefaultStreamingResourceManager(catalogBackend, storageBackend,
                s -> null,
                null
        );
    }

    @After
    public void tearDown() throws Exception {
        storageBackend.cleanup();
    }

    private Thread createChunkWriter(PipedOutputStream out, int startIndex, int endIndex) {
        return new Thread(() -> {
            try (PrintWriter writer = new PrintWriter(out)) {
                for (int i = startIndex; i < endIndex; i++) {
                    writer.print("chunk-" + i + "-a\n");
                    writer.flush();
                    Thread.sleep(500);
                    writer.print("chunk-" + i + "-b\n");
                    writer.flush();
                    Thread.sleep(500);
                }

                out.close(); // Normal completion

            } catch (IOException | InterruptedException e) {
                try {
                    out.close(); // Ensure EOF
                } catch (IOException ignored) {
                }
                throw new RuntimeException("Chunk writer failed", e);
            }
        });
    }

    @Test
    public void testHappyPathUploadWithListeners() throws Exception {
        final String resourceId = manager.registerNewResource(new StreamingResourceMetadata("test.txt", "text/plain"), null);

        final List<StreamingResourceStatus> listener1Events = new CopyOnWriteArrayList<>();
        final List<StreamingResourceStatus> listener2Events = new CopyOnWriteArrayList<>();

        manager.registerStatusListener(resourceId, listener1Events::add);
        Thread uploaderThread = new Thread(() -> {
            try {
                PipedOutputStream out1 = new PipedOutputStream();
                PipedInputStream in1 = new PipedInputStream(out1);
                Thread chunk1Writer = createChunkWriter(out1, 0, 2);

                PipedOutputStream out2 = new PipedOutputStream();
                PipedInputStream in2 = new PipedInputStream(out2);
                Thread chunk2Writer = createChunkWriter(out2, 2, 5);

                chunk1Writer.start();
                manager.writeChunk(resourceId, in1);
                chunk1Writer.join();

                chunk2Writer.start();
                manager.writeChunk(resourceId, in2);
                chunk2Writer.join();

                // Finalize
                manager.markCompleted(resourceId);

            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Uploader failed", e);
            }
        });

        uploaderThread.start();

        Thread.sleep(1000); // after 1s
        manager.registerStatusListener(resourceId, listener2Events::add);

        uploaderThread.join();

        assertFalse("Listener 1 should receive events", listener1Events.isEmpty());
        assertFalse("Listener 2 should receive events", listener2Events.isEmpty());

        StreamingResourceStatus final1 = listener1Events.get(listener1Events.size() - 1);
        StreamingResourceStatus final2 = listener2Events.get(listener2Events.size() - 1);

        assertEquals(StreamingResourceTransferStatus.COMPLETED, final1.getTransferStatus());
        assertEquals(final1, final2);
        assertEquals(100, final1.getCurrentSize().longValue());
    }

    @Test
    public void testUploadFailsWithIOException() throws Exception {
        final String resourceId = manager.registerNewResource(new StreamingResourceMetadata("test.txt", "text/plain"), null);

        final List<StreamingResourceStatus> listenerEvents = new CopyOnWriteArrayList<>();
        manager.registerStatusListener(resourceId, listenerEvents::add);

        Thread uploaderThread = new Thread(() -> {
            try {
                // === SINGLE CHUNK STREAM WITH FAILURE ===
                PipedOutputStream out = new PipedOutputStream();
                PipedInputStream in = new PipedInputStream(out);

                Thread chunkWriter = createChunkWriter(out, 0, 5);

                chunkWriter.start();
                FailingInputStream failingIn = new FailingInputStream(in, 35, false);
                manager.writeChunk(resourceId, failingIn); // will receive partial data
                chunkWriter.join();
            } catch (IOException | InterruptedException ignored) {
            }
        });

        uploaderThread.start();
        uploaderThread.join();

        assertFalse("Listener should receive events", listenerEvents.isEmpty());

        StreamingResourceStatus finalStatus = listenerEvents.get(listenerEvents.size() - 1);
        assertEquals(StreamingResourceTransferStatus.FAILED, finalStatus.getTransferStatus());
        assertNull("Failed upload should not have a final size", finalStatus.getCurrentSize());
    }

}
