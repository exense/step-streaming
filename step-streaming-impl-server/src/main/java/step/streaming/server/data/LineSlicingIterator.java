package step.streaming.server.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.PrimitiveIterator;

/**
 * An {@code Iterator<String>} that reads UTF-8 encoded lines from a given {@link InputStream},
 * slicing the input based on provided linebreak byte offsets.
 * <p>
 * This class is designed for efficient, streaming line extraction from a byte-oriented stream
 * (typically a bounded slice of a larger file), when the precise positions of linebreaks are known.
 * Each returned string includes the full content of one line, including the trailing linebreak byte (e.g. {@code \n}).
 *
 * <h3>Usage Assumptions</h3>
 * <ul>
 *   <li>The {@code InputStream} must contain a valid UTF-8-encoded slice of a larger resource.</li>
 *   <li>The {@code PrimitiveIterator.OfLong} must be adjusted to be relative to the given {@code InputStream} before passing to this class.</li>
 *   <li>Each offset in the iterator must point to the final byte of a line (typically the {@code \n} byte).</li>
 * </ul>
 *
 * <h3>Behavior</h3>
 * On each call to {@link #next()}, the iterator:
 * <ol>
 *   <li>Calculates the number of bytes from the previous offset (or 0 for the first line) up to and including the current break position.</li>
 *   <li>Reads exactly that number of bytes from the stream.</li>
 *   <li>Decodes the byte slice into a UTF-8 string.</li>
 * </ol>
 * This approach avoids materializing the full list of lines or positions, and ensures minimal memory usage with true streaming behavior.
 *
 */

public class LineSlicingIterator implements Iterator<String> {
    private final InputStream in;
    private final PrimitiveIterator.OfLong linebreakPositions;
    private long prevOffset = 0;

    public LineSlicingIterator(InputStream in, PrimitiveIterator.OfLong linebreakPositions) {
        this.in = in;
        this.linebreakPositions = linebreakPositions;
    }

    @Override
    public boolean hasNext() {
        return linebreakPositions.hasNext();
    }

    @Override
    public String next() {
        if (!hasNext()) throw new NoSuchElementException();

        long endOffset = linebreakPositions.nextLong();
        int len = (int) (endOffset + 1 - prevOffset);
        byte[] buffer = new byte[len];
        try {
            int read = in.readNBytes(buffer, 0, len);
            if (read != len) throw new IOException("Unexpected EOF while slicing line");
            prevOffset = endOffset + 1;
            return new String(buffer, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
