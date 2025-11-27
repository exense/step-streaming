package step.streaming.websocket.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * An InputStream that reads from a source InputStream and emits data at a throttled rate
 * over a specified duration. Useful for simulating slow network streams or uploads.
 */
@SuppressWarnings("NullableProblems")
public class TricklingDelegatingInputStream extends InputStream {

    private final InputStream source;
    private final long totalBytes;
    private final long durationMillis;
    private final long startTimeMillis;

    private long bytesEmitted = 0;

    /**
     * Constructs a TricklingDelegatingInputStream.
     *
     * @param source     the underlying InputStream to read from
     * @param totalBytes the total number of bytes expected from the source
     * @param duration   the duration over which to trickle the data
     * @param timeUnit   the time unit of the duration
     */
    public TricklingDelegatingInputStream(InputStream source, long totalBytes, long duration, TimeUnit timeUnit) {
        this.source = Objects.requireNonNull(source, "source InputStream cannot be null");
        if (totalBytes < 0) throw new IllegalArgumentException("totalBytes must be >= 0");
        if (duration < 0) throw new IllegalArgumentException("duration must be >= 0");

        this.totalBytes = totalBytes;
        this.durationMillis = timeUnit.toMillis(duration);
        this.startTimeMillis = System.currentTimeMillis();
    }

    @Override
    public int read() throws IOException {
        if (bytesEmitted >= totalBytes) return -1;

        throttle(1);
        int data = source.read();
        if (data != -1) bytesEmitted++;
        return data;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (bytesEmitted >= totalBytes) return -1;

        int bytesToRead = (int) Math.min(len, totalBytes - bytesEmitted);
        throttle(bytesToRead);

        int read = source.read(b, off, bytesToRead);
        if (read > 0) bytesEmitted += read;
        return read;
    }

    private void throttle(int bytesToEmit) throws IOException {
        if (durationMillis == 0) return;

        long elapsedMillis = System.currentTimeMillis() - startTimeMillis;
        long targetElapsed = (bytesEmitted + bytesToEmit) * durationMillis / totalBytes;
        long sleepTime = targetElapsed - elapsedMillis;

        if (sleepTime > 0) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during throttling", e);
            }
        }
    }

    @Override
    public void close() throws IOException {
        source.close();
    }
}
