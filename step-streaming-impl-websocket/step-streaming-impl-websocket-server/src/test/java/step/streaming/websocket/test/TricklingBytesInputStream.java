package step.streaming.websocket.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * An InputStream that emits a fixed number of random bytes over a specified duration.
 * Useful for simulating slow uploads or trickle streams with non-zero content.
 */
@SuppressWarnings("NullableProblems")
public class TricklingBytesInputStream extends InputStream {

    private final long totalBytes;
    private final long durationMillis;
    private final long startTimeMillis;
    private final Random random;

    private long bytesEmitted = 0;

    /**
     * Constructs a TricklingBytesInputStream with a default random generator.
     *
     * @param totalBytes the total number of bytes to emit
     * @param duration   the total duration over which the bytes should be emitted
     * @param timeUnit   the time unit for the duration
     */
    public TricklingBytesInputStream(long totalBytes, long duration, TimeUnit timeUnit) {
        this(totalBytes, duration, timeUnit, new Random());
    }

    /**
     * Constructs a TricklingBytesInputStream with a specific Random generator.
     *
     * @param totalBytes the total number of bytes to emit
     * @param duration   the total duration over which the bytes should be emitted
     * @param timeUnit   the time unit for the duration
     * @param random     the Random instance used to generate byte content
     */
    public TricklingBytesInputStream(long totalBytes, long duration, TimeUnit timeUnit, Random random) {
        if (totalBytes < 0) throw new IllegalArgumentException("totalBytes must be >= 0");
        if (duration < 0) throw new IllegalArgumentException("duration must be >= 0");

        this.totalBytes = totalBytes;
        this.durationMillis = timeUnit.toMillis(duration);
        this.startTimeMillis = System.currentTimeMillis();
        this.random = random;
    }

    @Override
    public int read() throws IOException {
        if (bytesEmitted >= totalBytes) return -1;

        throttle();
        bytesEmitted++;
        return random.nextInt(256);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (bytesEmitted >= totalBytes) return -1;

        int bytesLeft = (int) Math.min(len, totalBytes - bytesEmitted);
        throttle(bytesLeft);

        for (int i = 0; i < bytesLeft; i++) {
            b[off + i] = (byte) random.nextInt(256);
        }

        bytesEmitted += bytesLeft;
        return bytesLeft;
    }

    private void throttle() throws IOException {
        throttle(1);
    }

    private void throttle(int bytesToEmit) throws IOException {
        if (durationMillis == 0) return; // emit all immediately

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
}
