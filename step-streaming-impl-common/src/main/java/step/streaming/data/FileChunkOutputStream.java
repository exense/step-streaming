package step.streaming.data;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * An {@link OutputStream} that writes to a {@link File} starting at a given byte position.
 * <p>
 * The stream uses a {@link RandomAccessFile} in "rwd" mode to ensure both data and metadata
 * are written to disk, and allows appending or overwriting parts of a file at arbitrary offsets.
 */
@SuppressWarnings("NullableProblems") // silence IntelliJ-specific bogus warning
public class FileChunkOutputStream extends OutputStream {

    private final RandomAccessFile raf;
    private final boolean manualSync;

    /**
     * Creates a new chunked output stream for the given file.
     *
     * @param file          the file to write to
     * @param startPosition the position in the file to begin writing
     * @throws IOException if the file can't be opened or positioned
     */
    public FileChunkOutputStream(File file, long startPosition, boolean paranoidSync) throws IOException {
        if (startPosition < 0) {
            throw new IllegalArgumentException("Start position must be non-negative");
        }
        this.raf = new RandomAccessFile(file, paranoidSync ? "rwd" : "rw");
        this.manualSync = !paranoidSync;
        raf.seek(startPosition);
    }

    @Override
    public void write(int b) throws IOException {
        raf.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        raf.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        if (manualSync) {
            raf.getFD().sync();
        }
    }

    @Override
    public void close() throws IOException {
        flush();
        raf.close();
    }
}
