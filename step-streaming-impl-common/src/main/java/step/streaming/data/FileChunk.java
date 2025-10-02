package step.streaming.data;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Utility class for working with file ranges (chunks) as input and output streams.
 * <p>
 * Provides factory methods for creating streams that operate on specified regions
 * of a file, useful for resumable uploads or partial downloads.
 */
public class FileChunk {

    /**
     * Returns an {@link InputStream} that reads bytes from a file between two positions.
     *
     * @param file          the file to read
     * @param startPosition the starting byte position (inclusive)
     * @param endPosition   the ending byte position (exclusive)
     * @return a bounded {@link InputStream}
     * @throws IOException if the file cannot be accessed or positions are invalid
     */
    public static InputStream getInputStream(File file, long startPosition, long endPosition) throws IOException {
        return new FileChunkInputStream(file, startPosition, endPosition);
    }

    /**
     * Returns an {@link OutputStream} that writes to a file starting at a given byte position.
     *
     * @param file          the file to write to
     * @param startPosition the position to begin writing
     * @param paranoidSync  flag to indicate if whether writes have to be synced immediately. Not recommended for production unless you know why, as it severely affects performance.
     * @return a {@link FileChunkOutputStream}
     * @throws IOException if the file cannot be opened or the position is invalid
     */
    public static OutputStream getOutputStream(File file, long startPosition, boolean paranoidSync) throws IOException {
        return new FileChunkOutputStream(file, startPosition, paranoidSync);
    }

    // Utility class
    private FileChunk() {}
}
