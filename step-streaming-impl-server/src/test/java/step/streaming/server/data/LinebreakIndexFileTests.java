package step.streaming.server.data;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LinebreakIndexFileTests {

    private File rawIndexFile;

    @Before
    public void setup() throws Exception {
        rawIndexFile = File.createTempFile(this.getClass().getSimpleName(), ".idx");
    }

    @After
    public void teardown() throws Exception {
        if (!rawIndexFile.delete()) {
            System.err.println("Could not delete " + rawIndexFile);
            rawIndexFile.deleteOnExit();
        }
    }

    @Test
    public void smokeTest() throws Exception {
        LinebreakIndexFile index = new LinebreakIndexFile(rawIndexFile, 0, LinebreakIndexFile.Mode.WRITE);
        assertEquals(1, index.addLinebreakPosition(0L));
        assertEquals(2, index.addLinebreakPosition(1L));
        assertEquals(0, index.getLinebreakPosition(0));
        assertEquals(1, index.getLinebreakPosition(1));
        index.close();
    }

    @Test
    public void testTooLargeValue() throws Exception{
        LinebreakIndexFile index = new LinebreakIndexFile(rawIndexFile, 0, LinebreakIndexFile.Mode.WRITE);
        index.addLinebreakPosition(1L << 40 -1); // 0xFFFFFFFFFF, largest possible value
        assertEquals(1L << 40 -1, index.getLinebreakPosition(0));
        try {
            index.addLinebreakPosition(1L << 40); // 0x10000000000, too big
            fail("Expected an exception");
        } catch (IOException e) {
            assertEquals("Value out of range: 1099511627776", e.getMessage());
        }
        index.close();
    }

    @Test
    public void testWithBaseOffsetAndRange() throws Exception {
        long baseOffset = 42;
        LinebreakIndexFile index = new LinebreakIndexFile(rawIndexFile, baseOffset, LinebreakIndexFile.Mode.WRITE);
        assertEquals(1, index.addLinebreakPosition(23L));
        assertEquals(2, index.addLinebreakPosition(42L));
        index.close();
        index = new LinebreakIndexFile(rawIndexFile, baseOffset, LinebreakIndexFile.Mode.WRITE);
        assertEquals(3, index.addLinebreakPosition(3815L));
        assertEquals(4, index.addLinebreakPosition(4367L));
        index.close();
        index = new LinebreakIndexFile(rawIndexFile, baseOffset, LinebreakIndexFile.Mode.READ);
        assertEquals(4, index.getTotalEntries());
        assertEquals(4367 + baseOffset, index.getLinebreakPosition(3));
        assertEquals(List.of(), index.getLinebreakPositions(0, 0).collect(Collectors.toList()));
        assertEquals(List.of(23L, 42L), index.getLinebreakPositions(0, 2).map(l -> l - baseOffset).collect(Collectors.toList()));
        assertEquals(List.of(42L, 3815L, 4367L), index.getLinebreakPositions(1, 3).map(l -> l - baseOffset).collect(Collectors.toList()));
        try {
            index.addLinebreakPosition(0L);
            fail("Expected IOException - trying to write to read-only index");
        } catch (IOException expected) {
        }
        index.close();

    }

    @Test
    public void testConcurrentReadWhileWriting() throws Exception {
        final long baseOffset = 1000L;
        final int totalWrites = 50;
        final int assertTailLength = 15;

        // Create index file instance for writing
        LinebreakIndexFile writer = new LinebreakIndexFile(rawIndexFile, baseOffset, LinebreakIndexFile.Mode.WRITE);

        // Launch writer thread
        Thread writerThread = new Thread(() -> {
            try {
                for (int i = 0; i < totalWrites; i++) {
                    writer.addLinebreakPosition(i * 10L);
                    Thread.sleep(5); // simulate delay between writes
                }
            } catch (Exception e) {
                throw new RuntimeException("Writer thread failed", e);
            }
        });

        // Launch reader thread
        Thread readerThread = new Thread(() -> {
            try {
                LinebreakIndexFile longLivedReader = new LinebreakIndexFile(rawIndexFile, baseOffset, LinebreakIndexFile.Mode.READ);
                while (true) {
                    // This will check that the file grows over time (eventually reaching totalWrites entries),
                    // and will also check the last 15 (or fewer) entries against their expected value.
                    // Here we construct a new index file on every iteration, but the one created outside the loop should also
                    // return updated data
                    try (LinebreakIndexFile reader = new LinebreakIndexFile(rawIndexFile, baseOffset, LinebreakIndexFile.Mode.READ)) {
                        long available = reader.getTotalEntries();
                        if (available > 0) {
                            long tailStart = Math.max(0, available - assertTailLength);
                            long tailCount = available - tailStart;
                            List<Long> actual = reader
                                    .getLinebreakPositions(tailStart, (int) tailCount)
                                    .collect(Collectors.toList());

                            // System.out.println(actual); // in case someone wants to debug :-D -- examples:
                            // beginning: [1000, 1010, 1020, 1030, 1040, 1050]
                            // end: [1350, 1360, 1370, 1380, 1390, 1400, 1410, 1420, 1430, 1440, 1450, 1460, 1470, 1480, 1490]
                            for (int i = 0; i < actual.size(); i++) {
                                long expected = baseOffset + (tailStart + i) * 10L;
                                assertEquals("Mismatch at index " + (tailStart + i), expected, (long) actual.get(i));
                            }
                        }
                        // Now compare against the long-lived reader too
                        if (available > 0) {
                            long tailStart = Math.max(0, available - assertTailLength);
                            long tailCount = available - tailStart;
                            List<Long> actualFromLongLived = longLivedReader
                                    .getLinebreakPositions(tailStart, (int) tailCount)
                                    .collect(Collectors.toList());

                            for (int i = 0; i < actualFromLongLived.size(); i++) {
                                long expected = baseOffset + (tailStart + i) * 10L;
                                assertEquals("Mismatch from long-lived reader at index " + (tailStart + i),
                                        expected, (long) actualFromLongLived.get(i));
                            }
                        }

                        if (available == totalWrites) {
                            break;
                        }
                    }
                    Thread.sleep(10);
                }
                longLivedReader.close();
            } catch (Exception e) {
                throw new RuntimeException("Reader thread failed", e);
            }
        });

        writerThread.start();
        readerThread.start();

        writerThread.join();
        readerThread.join();

        writer.close();
    }


}
