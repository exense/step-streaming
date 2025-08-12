package step.streaming.server.data;

import step.streaming.server.LinebreakIndex;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.stream.Stream;

/**
 * A file representing a simple index about where linebreaks occur in a text file.
 * This implementation uses a trivial data layout, where line break positions are
 * expected to be added in a sequential fashion, and are stored in raw binary format
 * in a file (basically an on-disk array representation)
 * Each entry is saved in 5-bytes/40-bit format (big-endian), thus allowing for a
 * maximum offset of 1,099,511,627,775 i.e. 1 Terabyte.
 * <p>
 * Index files are created for a given datafile, and they reuse the datafile name and location,
 * simply adding an ".idx" extension.
 * <p>
 * INSTANCES OF THIS CLASS ARE NOT THREAD-SAFE. Operations on instances of this class are expected
 * to be used from a single thread only (at a time), and this should be perfectly fine for the envisaged
 * use case. If accessing the same index from multiple threads, simply use multiple instances.
 */
public class LinebreakIndexFile implements AutoCloseable, LinebreakIndex {
    public enum Mode {
        READ,
        WRITE
    }

    private static final String FILE_EXTENSION = ".idx";

    private static final long ENTRY_SIZE = 5L; // 40 bits -> positions up to 1 TB

    private final byte[] entryBuffer = new byte[5];

    private static final long MAX_ENTRY_VALUE = 0xFFFFFFFFFFL; // 40-bit max


    // This will always return a file (never null), but that file potentially does not exist.
    public static File getIndexFile(File dataFile) {
        return new File(dataFile.getParentFile(), dataFile.getName() + FILE_EXTENSION);
    }

    public static void createIfNeeded(File dataFile) throws IOException {
        File indexFile = getIndexFile(dataFile);
        if (!indexFile.exists()) {
            if (!indexFile.createNewFile()) {
                throw new IOException("Unable to create index file " + indexFile.getAbsolutePath());
            }
        }
        if (!indexFile.isFile() || !indexFile.canWrite()) {
            throw new IOException("Unable to write to index file " + indexFile.getAbsolutePath());
        }
    }

    public static LinebreakIndexFile openIfExists(File dataFile) throws IOException {
        File baseFile = getIndexFile(dataFile);
        if (!baseFile.exists()) {
            return null;
        }
        return new LinebreakIndexFile(baseFile, 0, Mode.READ);
    }

    public static LinebreakIndexFile openIfExistsForWrite(File dataFile, long baseOffset) throws IOException {
        File baseFile = getIndexFile(dataFile);
        if (!baseFile.exists()) {
            return null;
        }
        return new LinebreakIndexFile(baseFile, baseOffset, Mode.WRITE);
    }

    private final RandomAccessFile raf;
    private final long baseOffset; // only used when writing, irrelevant for reads

    LinebreakIndexFile(File rawIndexFile, long baseOffset, Mode mode) throws IOException {
        if (!rawIndexFile.exists()) {
            throw new IOException("Index file " + rawIndexFile.getAbsolutePath() + " does not exist");
        }
        this.raf = new RandomAccessFile(rawIndexFile, mode == Mode.WRITE ? "rw" : "r");
        this.raf.seek(raf.length());
        this.baseOffset = baseOffset;
    }

    private void encodeToBuffer(long value) throws IOException {
        if (value < 0 || value > MAX_ENTRY_VALUE) {
            throw new IOException("Value out of range: " + value);
        }

        entryBuffer[0] = (byte) ((value >> 32) & 0xFF);
        entryBuffer[1] = (byte) ((value >> 24) & 0xFF);
        entryBuffer[2] = (byte) ((value >> 16) & 0xFF);
        entryBuffer[3] = (byte) ((value >> 8) & 0xFF);
        entryBuffer[4] = (byte) (value & 0xFF);
    }

    private long decodeFromBuffer() throws IOException {
        return ((long) (entryBuffer[0] & 0xFF) << 32)
                | ((long) (entryBuffer[1] & 0xFF) << 24)
                | ((long) (entryBuffer[2] & 0xFF) << 16)
                | ((long) (entryBuffer[3] & 0xFF) << 8)
                | ((long) (entryBuffer[4] & 0xFF));
    }


    @Override
    public long getTotalEntries() {
        try {
            return raf.length() / ENTRY_SIZE;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long addLinebreakPosition(Long linebreakPosition) throws IOException {
        encodeToBuffer(linebreakPosition + baseOffset);
        raf.write(entryBuffer);
        return getTotalEntries();
    }

    @Override
    public long getLinebreakPosition(long index) throws IOException {
        long count = getTotalEntries();
        if (index < 0 || index >= count) {
            throw new IndexOutOfBoundsException("Index " + index + " out of bounds, must be between 0 and " + (count - 1));
        }
        raf.seek(index * ENTRY_SIZE);
        raf.readFully(entryBuffer);
        return decodeFromBuffer();
    }

    @Override
    public Stream<Long> getLinebreakPositions(long startIndex, long count) throws IOException {
        long entries = getTotalEntries();

        if (startIndex < 0 || count < 0 || startIndex + count > entries) {
            throw new IndexOutOfBoundsException(String.format(
                    "Invalid range: startIndex=%d, count=%d, totalEntries=%d. Valid range is [0, %d]",
                    startIndex, count, entries, entries
            ));
        }

        raf.seek(startIndex * ENTRY_SIZE);

        return Stream.generate(() -> {
            try {
                raf.readFully(entryBuffer);
                return decodeFromBuffer();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).limit(count);
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }
}
