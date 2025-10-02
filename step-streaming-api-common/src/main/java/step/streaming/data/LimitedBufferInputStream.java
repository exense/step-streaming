package step.streaming.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

/**
 * An InputStream decorator that clamps the maximum number of bytes read
 * per read(byte[], int, int) call to a configured maximum buffer size.
 * <p>
 * This is useful for controlling downstream buffer handling behavior
 * in situations where excessive buffering is actually unwanted.
 */
@SuppressWarnings("NullableProblems")
public class LimitedBufferInputStream extends InputStream {
    private final InputStream delegate;
    private final int maxReadSize;

    /**
     * Constructs an InputStream with limited buffering.
     *
     * @param delegate    the underlying input stream to wrap (must not be null)
     * @param maxReadSize the maximum number of bytes allowed per read operation (must be ≥ 1)
     */
    public LimitedBufferInputStream(InputStream delegate, int maxReadSize) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        if (maxReadSize < 1) throw new IllegalArgumentException("maxReadSize must be >= 1");
        this.maxReadSize = maxReadSize;
    }

    @Override
    public int read() throws IOException {
        return delegate.read(); // unchanged for single-byte read
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int clampedLen = Math.min(len, maxReadSize);
        return delegate.read(b, off, clampedLen);
    }

    @Override
    public long skip(long n) throws IOException {
        return delegate.skip(n);
    }

    @Override
    public int available() throws IOException {
        return delegate.available();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        delegate.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        delegate.reset();
    }

    @Override
    public boolean markSupported() {
        return delegate.markSupported();
    }
}
