package step.streaming.data;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;

/**
 * An {@link OutputStream} that writes to a {@link File} starting at a given byte position.
 * <p>
 * The stream uses a {@link RandomAccessFile} internally, and allows appending or overwriting parts of a file at arbitrary offsets.
 * If the {@code paranoidSync} parameter is {@code true}, the file is opened in "rwd" mode (instead of "rw") to ensure both data and metadata
 * are written to disk immediately, however this carries a severe performance impact and is not recommended for production use
 * unless you know why you want it.
 */
@SuppressWarnings("NullableProblems") // silence IntelliJ-specific bogus warning
public class FileChunkOutputStream extends OutputStream {

    private final RandomAccessFile raf;

    /**
     * Creates a new chunked output stream for the given file.
     *
     * @param file          the file to write to
     * @param startPosition the position in the file to begin writing
     * @param paranoidSync  if {@code true}, "rwd" mode is used for the backing {@link RandomAccessFile}, otherwise "rw".
     * @throws IOException if the file can't be opened or positioned
     */
    public FileChunkOutputStream(File file, long startPosition, boolean paranoidSync) throws IOException {
        if (startPosition < 0) {
            throw new IllegalArgumentException("Start position must be non-negative");
        }
        this.raf = new RandomAccessFile(file, paranoidSync ? "rwd" : "rw");
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
    public void close() throws IOException {
        flush();
        raf.close();
    }
}
