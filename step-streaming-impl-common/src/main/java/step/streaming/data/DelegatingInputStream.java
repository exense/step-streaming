package step.streaming.data;

import java.io.IOException;
import java.io.InputStream;

/**
 * An InputStream that delegates to a sequence of other InputStreams.
 * When the current chunk is exhausted (EOF), it asks {@code getNextDelegate()} for the next one.
 */
@SuppressWarnings("NullableProblems") // silence IntelliJ-specific bogus warning
public abstract class DelegatingInputStream<Delegate extends InputStream> extends InputStream {

    private boolean closed = false;
    protected Delegate delegate;

    public DelegatingInputStream() throws IOException {
        this.delegate = getNextDelegate();
    }

    /**
     * Provides the next chunk InputStream to read from after the current one is exhausted.
     * Return null to signal end-of-stream.
     */
    protected abstract Delegate getNextDelegate() throws IOException;

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        while (true) {
            if (delegate == null) {
                return -1;
            }
            int result = delegate.read(b, off, len);
            if (result != -1) {
                return result;
            }

            Delegate next = getNextDelegate();
            if (next == null) {
                return -1;
            }

            delegate.close();
            delegate = next;
        }
    }

    @Override
    public final int read() throws IOException {
        byte[] buffer = new byte[1];
        int r = read(buffer, 0, 1);
        return (r == -1) ? -1 : (buffer[0] & 0xFF);
    }

    @Override
    public final int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public long skip(long n) throws IOException {
        if (delegate == null) {
            return 0;
        }
        return delegate.skip(n);
    }

    @Override
    public int available() throws IOException {
        if (delegate == null) {
            return 0;
        }
        return delegate.available();
    }

    public final boolean isClosed() {
        return closed;
    }

    @Override
    public void close() throws IOException {
        if (!isClosed()) {
            this.closed = true;
            if (delegate != null) {
                delegate.close();
            }
        }
    }

    @Override
    public final synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException("mark/reset not supported");
    }

    @Override
    public final synchronized void reset() {
        throw new UnsupportedOperationException("mark/reset not supported");
    }

}
