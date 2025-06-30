package step.streaming.data;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * An OutputStream wrapper that periodically flushes and emits checkpoint notifications based on a configurable interval.
 * <p>
 * This stream delegates all output to an underlying {@link OutputStream}, and after each write,
 * checks whether the specified flush interval has elapsed. If so, it flushes the underlying stream
 * and invokes an optional listener with the number of total bytes written.
 */
@SuppressWarnings("NullableProblems") // silence IntelliJ-specific bogus warning
public class CheckpointingOutputStream extends OutputStream {
    /**
     * Default flush interval in milliseconds (1 second).
     */
    public static final long DEFAULT_FLUSH_INTERVAL_MILLIS = 1000;

    private final OutputStream delegate;
    private final long flushIntervalMillis;
    private final Consumer<Long> flushListener;
    private long lastFlushTime = 0;
    private long totalBytesWritten = 0;

    /**
     * Constructs a new CheckpointingOutputStream.
     *
     * @param delegate            the underlying OutputStream to write to (not null)
     * @param flushIntervalMillis the minimum time between flushes, in milliseconds (must be > 0)
     * @param flushListener       optional callback to be notified with total bytes written at each flush (may be null)
     * @throws NullPointerException     if {@code delegate} is null
     * @throws IllegalArgumentException if {@code flushIntervalMillis} is not positive
     */
    public CheckpointingOutputStream(OutputStream delegate, long flushIntervalMillis, Consumer<Long> flushListener) {
        this.delegate = Objects.requireNonNull(delegate);
        if (flushIntervalMillis <= 0) {
            throw new IllegalArgumentException("flushIntervalMillis must be greater than zero");
        }
        this.flushIntervalMillis = flushIntervalMillis;
        this.flushListener = flushListener;
    }

    /**
     * Writes a single byte and checks whether a flush is due.
     */
    @Override
    public void write(int b) throws IOException {
        delegate.write(b);
        totalBytesWritten++;
        maybeFlush();
    }

    /**
     * Writes a portion of a byte array and checks whether a flush is due.
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        delegate.write(b, off, len);
        totalBytesWritten += len;
        maybeFlush();
    }

    /**
     * Forces a flush to the underlying stream and notifies the listener (if any).
     */
    @Override
    public void flush() throws IOException {
        delegate.flush();
        if (flushListener != null) {
            flushListener.accept(totalBytesWritten);
        }
        lastFlushTime = System.currentTimeMillis();
    }

    private boolean closed = false;

    /**
     * Closes the stream, emitting a final checkpoint.
     */
    @Override
    public void close() throws IOException {
        // not all delegates support being closed multiple times (Websocket streams are an example),
        // so we gracefully handle this here.
        if (!closed) {
            flush();
            delegate.close();
            closed = true;
        }
    }

    /**
     * Checks if the flush interval has elapsed, and if so, flushes the stream and notifies the listener.
     */
    private void maybeFlush() throws IOException {
        long now = System.currentTimeMillis();
        if (now - lastFlushTime >= flushIntervalMillis) {
            flush();
        }
    }

    /**
     * Returns the total number of bytes written through this stream.
     *
     * @return total byte count
     */
    public long getTotalBytesWritten() {
        return totalBytesWritten;
    }
}
