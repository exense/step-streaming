package step.streaming.data;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * An InputStream that reads character data from an underlying source using a declared charset,
 * transcodes it into UTF-8, and returns the resulting bytes on-demand. If the source is
 * already in UTF-8 format, no transcoding is required and the source is used directly.
 * <p>
 * BOMs (Byte Order Marks) are explicitly detected and validated against the declared charset.
 * The stream emits no BOM in the output and supports partial, lazy consumption from slow or trickling sources.
 */
@SuppressWarnings("NullableProblems")
public class UTF8TranscodingTextInputStream extends InputStream {

    private InputStream sourceStream;
    private final Charset declaredCharset;
    private final CharsetEncoder encoder = StandardCharsets.UTF_8.newEncoder()
            .onMalformedInput(CodingErrorAction.REPLACE)
            .onUnmappableCharacter(CodingErrorAction.REPLACE);

    private final CharBuffer charBuffer = CharBuffer.allocate(1024);
    private final ByteBuffer byteBuffer = ByteBuffer.allocate(2048);

    private boolean passthrough = false;


    private Reader reader;
    private boolean endOfInputReached = false;
    private boolean initialized = false;

    /**
     * Constructs a new {@code UTF8TranscodingTextInputStream} that reads character data from the given
     * {@code InputStream}, decodes it using the specified {@code Charset}, and encodes the result
     * as UTF-8 bytes.
     * <p>
     * This stream is useful when you have input in a known encoding (such as ISO-8859-1, UTF-16LE, etc.)
     * and you want to convert it to UTF-8 on-the-fly while reading it as a standard {@code InputStream}.
     * <p>
     * If the underlying input contains a Byte Order Mark (BOM), it will be detected and:
     * <ul>
     *   <li>Silently skipped if it matches the declared charset</li>
     *   <li>Cause an {@link IllegalArgumentException} if it conflicts with the declared charset</li>
     * </ul>
     * No BOM will be included in the UTF-8 output.
     *
     * @param sourceStream    The raw byte-oriented input stream to read from. Must not be {@code null}.
     * @param declaredCharset The character encoding of the input stream. This must match any BOM prefix
     *                        present in the stream; otherwise, an exception will be thrown.
     *                        If no BOM is present, this charset is used as-is.
     * @throws NullPointerException     if either argument is {@code null}
     * @throws IllegalArgumentException if a BOM is present and does not match the declared charset
     */
    public UTF8TranscodingTextInputStream(InputStream sourceStream, Charset declaredCharset) {
        this.sourceStream = Objects.requireNonNull(sourceStream);
        this.declaredCharset = Objects.requireNonNull(declaredCharset);
        charBuffer.limit(0);
        byteBuffer.limit(0);
    }

    private boolean charsetSupportsBOM(Charset charset) {
        return charset.equals(StandardCharsets.UTF_8)
                || charset.equals(StandardCharsets.UTF_16)
                || charset.equals(StandardCharsets.UTF_16LE)
                || charset.equals(StandardCharsets.UTF_16BE);
    }

    // This method is called lazily on read (and will only perform actual initialization on first read).
    // For UTF encoded files, a BOM (Byte Order Mark) might be present at the beginning
    // of the file. If so, evaluate it and check if it matches the declared charset.
    // Note that after transcoding, the output will NOT contain any BOM as per current best practices for UTF-8.
    private void initialize() throws IOException {
        if (initialized) return;

        PushbackInputStream pushback = new PushbackInputStream(sourceStream, 3);
        Charset actualCharset = declaredCharset;

        // only perform BOM detection on UTF input
        if (charsetSupportsBOM(declaredCharset)) {
            byte[] bom = new byte[3];
            int read = 0;

            // Read up to 3 bytes
            while (read < 3) {
                int b = pushback.read();
                if (b == -1) break;
                bom[read++] = (byte) b;
            }

            int skip = 0;
            if (read >= 3 && (bom[0] & 0xFF) == 0xEF && (bom[1] & 0xFF) == 0xBB && (bom[2] & 0xFF) == 0xBF) {
                // UTF-8 BOM
                if (!actualCharset.equals(StandardCharsets.UTF_8)) {
                    throw new IllegalArgumentException("BOM indicates UTF-8, but declared charset is: " + actualCharset.name());
                }
                skip = 3;
            } else if (read >= 2 && (bom[0] & 0xFF) == 0xFF && (bom[1] & 0xFF) == 0xFE) {
                // UTF-16LE BOM
                if (!actualCharset.equals(StandardCharsets.UTF_16LE) && !actualCharset.equals(StandardCharsets.UTF_16)) {
                    throw new IllegalArgumentException("BOM indicates UTF-16LE, but declared charset is: " + actualCharset.name());
                }
                skip = 2;
            } else if (read >= 2 && (bom[0] & 0xFF) == 0xFE && (bom[1] & 0xFF) == 0xFF) {
                // UTF-16BE BOM
                if (!actualCharset.equals(StandardCharsets.UTF_16BE) && !actualCharset.equals(StandardCharsets.UTF_16)) {
                    throw new IllegalArgumentException("BOM indicates UTF-16BE, but declared charset is: " + actualCharset.name());
                }
                skip = 2;
            }

            // Push back any unconsumed bytes, but discard the bytes to be skipped (the detected BOM)
            if (read > skip) {
                pushback.unread(bom, skip, read - skip);
            }

            // Optimization: if source is already UTF-8, directly passthrough instead of transcoding
            if (actualCharset.equals(StandardCharsets.UTF_8)) {
                passthrough = true;
                sourceStream = pushback;
                initialized = true;
                return;
            }
        }

        reader = new InputStreamReader(pushback, actualCharset);
        initialized = true;
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int n = read(b, 0, 1);
        return (n == -1) ? -1 : (b[0] & 0xFF);
    }

    @Override
    public int read(byte[] outputBytes, int outputOffset, int outputLength) throws IOException {
        if (outputBytes == null) throw new NullPointerException();
        if (outputOffset < 0 || outputLength < 0 || outputOffset + outputLength > outputBytes.length) {
            throw new IndexOutOfBoundsException();
        }

        // Ensure stream is initialized (BOM checked, reader created, buffers allocated)
        initialize();
        if (passthrough) {
            return sourceStream.read(outputBytes, outputOffset, outputLength);
        }

        int totalWritten = 0;

        // Try to fill the caller's buffer with up to 'outputLength' bytes
        while (totalWritten < outputLength) {
            // If we have no more encoded bytes available, try to refill the buffer
            if (!byteBuffer.hasRemaining()) {
                if (!refillByteBuffer()) {
                    break; // No more input available; end of stream
                }
            }

            // Determine how many bytes we can safely copy this round
            int toCopy = Math.min(outputLength - totalWritten, byteBuffer.remaining());

            // Drain encoded UTF-8 bytes from our internal buffer into the caller's output buffer
            byteBuffer.get(outputBytes, outputOffset + totalWritten, toCopy);

            totalWritten += toCopy;
        }

        // If we couldn't write any bytes and hit EOF, return -1 per InputStream spec
        return (totalWritten == 0) ? -1 : totalWritten;
    }

    private boolean refillByteBuffer() throws IOException {
        // If input is fully consumed and nothing left in charBuffer, we’re done
        if (endOfInputReached && !charBuffer.hasRemaining()) return false;

        // Prepare charBuffer for more input: move unread chars to beginning
        charBuffer.compact();

        // Read more characters from the underlying reader
        int charsRead = reader.read(charBuffer);
        if (charsRead == -1) {
            endOfInputReached = true; // Mark EOF reached for next time
        }

        // Flip buffer to prepare it for reading by encoder
        charBuffer.flip();

        // Prepare byteBuffer to receive new UTF-8-encoded bytes
        byteBuffer.clear();

        // Encode characters into byteBuffer
        encoder.encode(charBuffer, byteBuffer, endOfInputReached);

        // Flip byteBuffer to reading mode so it can be drained by read()
        byteBuffer.flip();

        // Return whether there’s anything available to read now
        return byteBuffer.hasRemaining();
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        } else {
            sourceStream.close();
        }
    }
}
