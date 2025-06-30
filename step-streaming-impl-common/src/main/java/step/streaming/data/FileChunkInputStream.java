package step.streaming.data;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 * An {@link InputStream} that reads a defined byte range from a {@link File}
 * using a backing {@link RandomAccessFile} in read-only mode.
 * <p>
 * The stream reads from {@code start} (inclusive) up to {@code end} (exclusive),
 * and automatically closes the file when done.
 */
@SuppressWarnings("NullableProblems") // silence IntelliJ-specific bogus warning
public class FileChunkInputStream extends InputStream {

    private final RandomAccessFile raf;
    private final long end;
    private long position;

    /**
     * Constructs a new chunked input stream over the given file.
     *
     * @param file  the source file to read from
     * @param start the start byte offset (inclusive)
     * @param end   the end byte offset (exclusive)
     * @throws IOException              if the file cannot be read or bounds are invalid
     * @throws IllegalArgumentException if start < 0, end < start, or end exceeds file length
     */
    public FileChunkInputStream(File file, long start, long end) throws IOException {
        if (start < 0 || end < start) {
            throw new IllegalArgumentException("Invalid start/end positions");
        }

        this.raf = new RandomAccessFile(file, "r");

        if (end > raf.length()) {
            try {
                raf.close();
            } catch (IOException ignored) {}
            throw new IllegalArgumentException("End position exceeds file length");
        }

        this.end = end;
        this.position = start;
        raf.seek(start);
    }

    /**
     * Reads a single byte or returns -1 at EOF.
     */
    @Override
    public int read() throws IOException {
        if (position >= end) return -1;
        int b = raf.read();
        if (b != -1) position++;
        return b;
    }

    /**
     * Reads up to {@code len} bytes into the buffer, bounded by the end position.
     */
    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (position >= end) return -1;

        long remaining = end - position;
        int toRead = (int) Math.min(len, remaining);
        int bytesRead = raf.read(b, off, toRead);

        if (bytesRead > 0) {
            position += bytesRead;
        }

        return bytesRead;
    }

    /**
     * Skips up to {@code n} bytes, bounded by the end position.
     */
    @Override
    public long skip(long n) throws IOException {
        long remaining = end - position;
        long toSkip = Math.min(n, remaining);
        position += toSkip;
        raf.seek(position);
        return toSkip;
    }

    /**
     * Returns the number of bytes remaining to be read.
     */
    @Override
    public int available() {
        return (int) Math.min(Integer.MAX_VALUE, end - position);
    }

    /**
     * Closes the stream and its underlying file.
     */
    @Override
    public void close() throws IOException {
        raf.close();
    }

    /**
     * This stream does not support mark/reset.
     */
    @Override
    public boolean markSupported() {
        return false;
    }
}
