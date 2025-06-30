package step.streaming.data;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

// Provides an InputStream over a file that is (potentially) still growing.
// This stream will behave like a normal InputStream, except that
// it will NOT signal EOF when the end of the file is reached.
/**
 * An {@link java.io.InputStream} implementation that reads from a file which may still be growing.
 * <p>
 * Unlike a standard {@link java.io.FileInputStream}, this stream does not return EOF (end-of-file)
 * immediately upon reaching the current end of the file. Instead, it waits until the associated
 * {@link EndOfInputSignal} indicates that the file is fully written and no more data will arrive.
 * In other words, it will continue to block, waiting for the file to grow,
 * and only signal EOF when both the end of the file was reached, and
 * the given finishedSignal indicates success. If the finishedSignal indicates
 * an error, an IOException is thrown instead.
 * <p>
 * This is useful for streaming scenarios where data is being written to a file while it is being read,
 * such as in live uploads or log tailing use cases.
 */
@SuppressWarnings("NullableProblems") // silence IntelliJ-specific bogus warning
public class LiveFileInputStream extends EndOfInputRequiringInputStream {
    private final RandomAccessFile raf;
    private final EndOfInputSignal finishedSignal;
    private final long pollIntervalMillis;
    private boolean isDone = false;

    /**
     * Constructs a new {@code LiveFileInputStream} for the given file, using the provided signal
     * to determine when the end of the input is truly reached.
     *
     * @param file              the file to read from; must not be {@code null}
     * @param finishedSignal    the signal that determines when the file is finished being written
     * @param pollIntervalMillis the polling interval in milliseconds to check the signal when at EOF
     * @throws IOException if the file cannot be opened for reading
     */
    public LiveFileInputStream(File file, EndOfInputSignal finishedSignal, long pollIntervalMillis) throws IOException {
        this.raf = new RandomAccessFile(Objects.requireNonNull(file), "r");
        this.finishedSignal = Objects.requireNonNull(finishedSignal);
        this.pollIntervalMillis = pollIntervalMillis;
    }

    @Override
    public EndOfInputSignal getEndOfInputSignal() {
        return finishedSignal;
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int result = read(b, 0, 1);
        return result == -1 ? -1 : (b[0] & 0xFF);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        while (true) {
            long currentPointer = raf.getFilePointer();
            long fileLength = raf.length();

            if (fileLength > currentPointer) {
                return raf.read(b, off, len);
            }

            if (!isDone) {
                try {
                    // Wait up to pollIntervalMillis for the finished signal
                    finishedSignal.get(pollIntervalMillis, TimeUnit.MILLISECONDS);
                    // the file may have grown while waiting! If so, perform another iteration.
                    isDone = raf.length() == fileLength;
                } catch (java.util.concurrent.TimeoutException e) {
                    // Future not done yet — this is expected, continue to next loop iteration
                } catch (ExecutionException e) {
                    // This is where user-supplied exceptions are handled
                    throw new IOException(e.getCause());
                } catch (Exception e) {
                    throw new IOException(e.getMessage(), e);
                }
            }

            if (isDone) {
                return -1; // End of file
            }
        }
    }

    @Override
    public void close() throws IOException {
        raf.close();
    }
}
