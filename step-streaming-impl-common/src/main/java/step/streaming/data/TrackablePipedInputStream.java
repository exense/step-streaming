package step.streaming.data;

import java.io.IOException;
import java.io.PipedInputStream;

/**
 * An extension of {@link PipedInputStream} that tracks whether the stream has been explicitly closed.
 */
public class TrackablePipedInputStream extends PipedInputStream {

    private volatile boolean closed = false;

    /**
     * Constructs a new {@code TrackablePipedInputStream} with the default buffer size.
     */
    public TrackablePipedInputStream() {
        super();
    }

    /**
     * Constructs a new {@code TrackablePipedInputStream} with the specified buffer size.
     *
     * @param bufferSize the size of the circular buffer
     */
    public TrackablePipedInputStream(int bufferSize) {
        super(bufferSize);
    }

    /**
     * Closes this input stream and marks it as closed.
     * Once closed, {@link #isClosed()} will return {@code true}.
     *
     * @throws IOException if an I/O error occurs
     */
    @Override
    public void close() throws IOException {
        closed = true;
        super.close();
    }

    /**
     * Returns whether this stream has been explicitly closed.
     *
     * @return {@code true} if {@link #close()} has been called; {@code false} otherwise
     */
    public boolean isClosed() {
        return closed;
    }
}
