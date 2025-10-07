package step.streaming.server.test;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An InputStream that simulates an IOException after a given number of bytes or optionally on close().
 * Useful for testing failure scenarios in streaming pipelines.
 */
public class FailingInputStream extends FilterInputStream {

    private final long failAfterBytes;
    private final boolean failOnClose;
    private long bytesRead = 0;
    private boolean alreadyFailed = false;

    /**
     * Constructs a FailingInputStream.
     *
     * @param in             The underlying input stream.
     * @param failAfterBytes The number of bytes to allow before simulating an IOException.
     * @param failOnClose    Whether to simulate an IOException when close() is called.
     */
    public FailingInputStream(InputStream in, long failAfterBytes, boolean failOnClose) {
        super(in);
        this.failAfterBytes = failAfterBytes;
        this.failOnClose = failOnClose;
    }

    @Override
    public int read() throws IOException {
        checkFailure();
        int result = super.read();
        if (result != -1) {
            bytesRead++;
            checkFailure();
        }
        return result;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkFailure();

        long remaining = failAfterBytes - bytesRead;
        if (remaining <= 0) {
            throwFail("Simulated failure before read");
        }

        int toRead = (int) Math.min(len, remaining);
        int read = super.read(b, off, toRead);

        if (read > 0) {
            bytesRead += read;
            checkFailure();
        }

        return read;
    }

    @Override
    public void close() throws IOException {
        if (failOnClose && !alreadyFailed) {
            throwFail("Simulated failure on close");
        }
        super.close();
    }

    private void checkFailure() throws IOException {
        if (!alreadyFailed && bytesRead >= failAfterBytes) {
            throwFail("Simulated failure after " + bytesRead + " bytes");
        }
    }

    private void throwFail(String message) throws IOException {
        alreadyFailed = true;
        throw new IOException(message);
    }

    /**
     * @return Total number of bytes successfully read before failure.
     */
    public long getBytesRead() {
        return bytesRead;
    }

    /**
     * @return True if failure has already occurred.
     */
    public boolean hasFailed() {
        return alreadyFailed;
    }
}
