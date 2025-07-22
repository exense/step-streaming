package step.streaming.server.data;

import step.streaming.data.util.ThrowingConsumer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

/**
 * An OutputStream decorator that detects linebreaks ({@code '\\n'} bytes) in the written stream
 * and reports their absolute byte position to a provided Consumer.
 */
@SuppressWarnings("NullableProblems")
public class LinebreakDetectingOutputStream extends OutputStream {

    private final OutputStream delegate;
    private final ThrowingConsumer<Long> linebreakConsumer;
    private long bytePosition = 0;

    /**
     * Creates a new linebreak-detecting OutputStream.
     *
     * @param delegate          the underlying OutputStream to write to
     * @param linebreakConsumer a callback that receives the byte position of each '\\n'
     */
    public LinebreakDetectingOutputStream(OutputStream delegate, ThrowingConsumer<Long> linebreakConsumer) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.linebreakConsumer = Objects.requireNonNull(linebreakConsumer, "linebreakConsumer must not be null");
    }

    @Override
    public void write(int b) throws IOException {
        if ((b & 0xFF) == '\n') {
            try {
                linebreakConsumer.accept(bytePosition);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
        delegate.write(b);
        bytePosition++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        for (int i = 0; i < len; i++) {
            int value = b[off + i] & 0xFF;
            if (value == '\n') {
                try {
                    linebreakConsumer.accept(bytePosition + i);
                } catch (Exception e) {
                    throw new IOException(e);
                }
            }
        }
        delegate.write(b, off, len);
        bytePosition += len;
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
