package step.streaming.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;
import step.streaming.common.QuotaExceededException;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.server.data.LinebreakIndexFile;
import step.streaming.server.test.FailingInputStream;
import step.streaming.server.test.InMemoryCatalogBackend;
import step.streaming.server.test.TestingStorageBackend;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.PrintWriter;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class DefaultStreamingResourceManagerTest {

    private static final String TEXT_PLAIN = "text/plain";
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
        final String resourceId = manager.registerNewResource(new StreamingResourceMetadata("test.txt", TEXT_PLAIN, true), null);

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
                manager.writeChunk(resourceId, in1, false);
                chunk1Writer.join();

                chunk2Writer.start();
                manager.writeChunk(resourceId, in2, true);
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
        assertEquals(100, final1.getCurrentSize());
    }

    @Test
    public void testUploadFailsWithIOException() throws Exception {
        final String resourceId = manager.registerNewResource(new StreamingResourceMetadata("test.txt", TEXT_PLAIN, true), null);

        final List<StreamingResourceStatus> listenerEvents = new CopyOnWriteArrayList<>();
        manager.registerStatusListener(resourceId, listenerEvents::add);

        Thread uploaderThread = new Thread(() -> {
            try {
                // === SINGLE CHUNK STREAM WITH FAILURE ===
                PipedOutputStream out = new PipedOutputStream();
                PipedInputStream in = new PipedInputStream(out);

                Thread chunkWriter = createChunkWriter(out, 0, 5);

                chunkWriter.start();
                FailingInputStream failingIn = new FailingInputStream(in, 37, false);
                manager.writeChunk(resourceId, failingIn, true); // will receive partial data
                chunkWriter.join();
            } catch (IOException | InterruptedException ignored) {
            }
        });

        uploaderThread.start();
        uploaderThread.join();

        assertFalse("Listener should receive events", listenerEvents.isEmpty());

        StreamingResourceStatus finalStatus = listenerEvents.getLast();
        assertEquals(StreamingResourceTransferStatus.FAILED, finalStatus.getTransferStatus());
        // we still expect failed resources to keep their last "successful" size
        assertEquals(30L, finalStatus.getCurrentSize());
    }

    @Test
    public void testLineIndexingWithFinalLinebreak() throws Exception {
        String id = uploadAndCheckSizeAndLinebreaks("fileWithFinalLineBreak.txt", 81, 5);
        // we'll do some of the edge case tests in here
        assertException(() -> manager.getLinebreakPositions(id, 0, -1), "count must not be negative");
        assertException(() -> manager.getLinebreakPositions(id, -1, 2), "Linebreak index out of bounds: -1; acceptable values: [0, 5[");
        assertException(() -> manager.getLinebreakPositions(id, 5, 2), "Linebreak index out of bounds: 5; acceptable values: [0, 5[");
        assertException(() -> manager.getLinebreakPositions(id, 4, 2), "Starting index + count exceeds number of entries: 4 + 2 > 5");
        // requesting an empty stream (index doesn't really matter, should just be within bounds)
        assertEquals(List.of(), manager.getLinebreakPositions(id, 2, 0));
        assertEquals(List.of(55L, 60L, 65L, 71L, 80L), manager.getLinebreakPositions(id, 0, 5));
        assertEquals(List.of(65L, 71L), manager.getLinebreakPositions(id, 2, 2));
        assertEquals("Note: This file uses CR/LF line breaks (Windows-style)\r\n", manager.getLines(id, 0, 1).get(0));
        assertEquals("uno\r\n", manager.getLines(id, 1, 1).get(0));
        assertEquals(List.of("dos\r\n", "tres\r\n"), manager.getLines(id, 2, 2));
    }


    @Test
    public void testLineIndexingWithoutFinalLinebreak() throws Exception {
        String id = uploadAndCheckSizeAndLinebreaks("fileWithoutFinalLineBreak.txt", 82, 5L);
        // here the manager has to manage the missing last linebreak by itself (index only contains 4)
        assertEquals(List.of(50L, 54L, 58L, 64L, 81L), manager.getLinebreakPositions(id, 0, 5));
        assertEquals(List.of("two\n", "three\n", "foooooooooooooour" /* note: no trailing LB */), manager.getLines(id, 2, 3));

    }

    @Test
    public void testLineIndexingSingleLineWithFinalLinebreak() throws Exception {
        String id = uploadAndCheckSizeAndLinebreaks("singleLineWithLinebreak.txt", 23, 1);
        assertEquals(List.of(22L), manager.getLinebreakPositions(id, 0, 1));
        assertEquals(List.of("This is a single line.\n"), manager.getLines(id, 0, 1));
    }

    @Test
    public void testLineIndexingSingleLineWithoutFinalLinebreak() throws Exception {
        String id = uploadAndCheckSizeAndLinebreaks("singleLineWithoutLinebreak.txt", 43, 1);
        assertEquals(List.of(42L), manager.getLinebreakPositions(id, 0, 1));
        assertEquals(List.of("This is a single line without a line break."), manager.getLines(id, 0, 1));
    }

    @Test
    public void testLineIndexingEmpty() throws Exception {
        uploadAndCheckSizeAndLinebreaks("empty.txt", 0, 0);
    }

    @Test
    public void testLineIndexingLinebreakOnly() throws Exception {
        String id = uploadAndCheckSizeAndLinebreaks("linebreakOnly.txt", 1, 1);
        assertEquals(List.of(0L), manager.getLinebreakPositions(id, 0, 1));
        assertEquals(List.of("\n"), manager.getLines(id, 0, 1));
    }

    @Test
    public void testDeletion() throws Exception {
        // we just use an existing method to generate an upload
        assertEquals(0, catalogBackend.catalog.size());
        String id = uploadAndCheckSizeAndLinebreaks("linebreakOnly.txt", 1, 1);
        assertEquals(1, catalogBackend.catalog.size());
        assertEquals(1, storageBackend.getCurrentSize(id));
        try (LinebreakIndexFile index = storageBackend.getLinebreakIndex(id)) {
            assertNotNull(index);
        }

        manager.deleteResource(id);
        assertEquals(0, catalogBackend.catalog.size());
        assertNull(storageBackend.getLinebreakIndex(id));
        // non-existing resources return 0 by design in storage (but file is actually deleted)
        assertEquals(0, storageBackend.getCurrentSize(id));
        File f = storageBackend.getTempDirectory();
        // storage directory should be empty again after deletion of last resource
        assertEquals(0, Objects.requireNonNull(f.listFiles()).length);
    }

    @Test
    public void testSuccessfulZeroByteUploadsAreNotPersisted() throws Exception {
        assertEquals(0, storageBackend.countFiles());
        assertEquals(0, catalogBackend.catalog.size());
        String resource = uploadAndCheckSizeAndLinebreaks("empty.txt", 0, 0);
        assertEquals(1, catalogBackend.catalog.size());
        // This is the magic check: we do NOT actually want the backend to have persisted the files, so it should still have 0 files.
        assertEquals(0, storageBackend.countFiles());

        // but the resource should exist, and we should be able to retrieve the (0-byte) content
        assertEquals(new StreamingResourceStatus(StreamingResourceTransferStatus.COMPLETED, 0L, 0L), manager.getStatus(resource));
        try (InputStream is = manager.openStream(resource, 0, 0)) {
            assertEquals(0, is.available());
        }
        assertEquals(0, manager.getLines(resource, 0, 0).size());

        manager.deleteResource(resource);
        assertThrows(IllegalArgumentException.class, () -> manager.getStatus(resource));
        assertThrows(IllegalArgumentException.class, () -> manager.getLines(resource, 0, 0).size());
        assertThrows(IllegalArgumentException.class, () -> manager.openStream(resource, 0, 0));
    }

    @Test
    public void testUnsuccessfulZeroByteUploadsAreNotPersisted() throws Exception {
        assertEquals(0, storageBackend.countFiles());
        assertEquals(0, catalogBackend.catalog.size());
        String resourceId = manager.registerNewResource(new StreamingResourceMetadata("test.txt", TEXT_PLAIN, true), null);
        InputStream data = new ByteArrayInputStream("broken".getBytes());
        // it doesn't really matter after how many bytes we fail, the entire chunk won't be transferred
        // because the IO copy works with a 16kB buffer (see InputStream.transferTo), and it fails while filling the buffer
        InputStream broken = new FailingInputStream(data, 3, false);
        try {
            // "happy" (5 bytes) will be written, "broken" won't
            manager.writeChunk(resourceId, new ByteArrayInputStream("happy".getBytes()), false);
            manager.writeChunk(resourceId, broken, true);
            fail("Expected IOException to be thrown");
        } catch (IOException expected) {
        }
        assertEquals(1, catalogBackend.catalog.size());
        assertEquals(2, storageBackend.countFiles()); // data file + index file = 2 files per indexed text upload
        assertEquals(new StreamingResourceStatus(StreamingResourceTransferStatus.FAILED, 5L, 1L), manager.getStatus(resourceId));
        assertEquals(List.of("happy"), manager.getLines(resourceId, 0, 1));
        // Now, write a new resource that breaks immediately on the first chunk. We expect it to remain at 0 written bytes, and therefore NOT to be saved to a file.

        resourceId = manager.registerNewResource(new StreamingResourceMetadata("test.txt", TEXT_PLAIN, true), null);
        data = new ByteArrayInputStream("broken".getBytes());
        broken = new FailingInputStream(data, 3, false);
        try {
            manager.writeChunk(resourceId, broken, true);
            fail("Expected IOException to be thrown");
        } catch (IOException expected) {
        }
        // Catalog contains new entry
        assertEquals(2, catalogBackend.catalog.size());
        // But backend still contains 2 files, NOT 4! 0-byte files are optimized away
        assertEquals(2, storageBackend.countFiles());
        assertEquals(new StreamingResourceStatus(StreamingResourceTransferStatus.FAILED, 0L, 0L), manager.getStatus(resourceId));
    }

    private String uploadAndCheckSizeAndLinebreaks(String fileName, long expectedSize, long expectedNumberOfLines) throws QuotaExceededException, IOException {
        final String resourceId = manager.registerNewResource(new StreamingResourceMetadata("test.txt", TEXT_PLAIN, true), null);
        InputStream is = getClass().getClassLoader().getResourceAsStream(fileName);
        long size = manager.writeChunk(resourceId, is, true);
        assertEquals(expectedSize, size);
        manager.markCompleted(resourceId);
        StreamingResourceStatus expected = new StreamingResourceStatus(StreamingResourceTransferStatus.COMPLETED, expectedSize, expectedNumberOfLines);
        assertEquals(expected, manager.getStatus(resourceId));
        return resourceId;
    }

    private void assertException(ThrowingRunnable runnable, String message) {
        boolean failTest = true;
        try {
            runnable.run();
        } catch (Throwable e) {
            failTest = false;
            assertEquals(message, e.getMessage());
        } finally {
            if (failTest) {
                fail("Expected exception with message: " + message);
            }
        }
    }
}
