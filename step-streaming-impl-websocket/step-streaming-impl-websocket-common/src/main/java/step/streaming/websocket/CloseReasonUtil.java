package step.streaming.websocket;

import jakarta.websocket.CloseReason;

import java.nio.charset.StandardCharsets;

/**
 * Utility class for safely constructing {@link CloseReason} instances.
 * <p>
 * The standard {@link CloseReason} constructor enforces a maximum length
 * of 123 bytes for the UTF-8 encoded reason phrase. If the provided string
 * exceeds this limit, the constructor throws an {@link IllegalArgumentException}.
 * </p>
 *
 * <p>
 * This utility ensures that you always get a valid {@code CloseReason}:
 * it will truncate the reason text at a character boundary such that
 * the UTF-8 encoding is guaranteed to be ≤ 123 bytes, and then construct
 * the {@code CloseReason}. This avoids runtime exceptions due to overly
 * long reason phrases (yes, it can happen easily if you don't pay attention).
 * </p>
 *
 */
public class CloseReasonUtil {

    /**
     * Maximum byte length of the reason phrase, as defined in the specification.
     */
    public static final int MAX_REASON_BYTES = 123;


    /**
     * Creates a new {@link CloseReason} with the given code and reason phrase,
     * truncating the reason if necessary so that its UTF-8 encoded form does
     * not exceed the 123-byte limit imposed by the WebSocket specification.
     *
     * <p>
     * Behavior:
     * <ul>
     *   <li>If {@code closeCode} is {@code null}, an {@link IllegalArgumentException} is thrown.</li>
     *   <li>If {@code reason} is null or an empty string, the resulting {@code CloseReason}
     *       will have a {@code null} reason phrase (consistent with the JSR 356 spec).</li>
     *   <li>If the UTF-8 encoded {@code reason} is ≤ 123 bytes, it is used as-is.</li>
     *   <li>If it is longer, the string is truncated at a code point boundary so that
     *       the UTF-8 encoded byte length is ≤ 123, and that truncated string is used.</li>
     * </ul>
     * </p>
     *
     * @param closeCode the {@link CloseReason.CloseCode} (must not be {@code null})
     * @param reason    the reason phrase (must not be {@code null})
     * @return a {@link CloseReason} guaranteed to satisfy the UTF-8 byte limit
     * @throws IllegalArgumentException if {@code closeCode} is {@code null}
     */
    public static CloseReason makeSafeCloseReason(CloseReason.CloseCode closeCode, String reason) {
        if (closeCode == null) {
            throw new IllegalArgumentException("closeCode cannot be null");
        }
        if (reason == null || reason.isEmpty()) {
            return new CloseReason(closeCode, null);
        }

        // Fast path: already short enough
        if (reason.getBytes(StandardCharsets.UTF_8).length <= MAX_REASON_BYTES) {
            return new CloseReason(closeCode, reason);
        }

        // Walk code points until we would exceed the limit
        int byteCount = 0;
        int charIndex = 0;
        final int len = reason.length();
        while (charIndex < len) {
            int cp = reason.codePointAt(charIndex);
            int cpBytes = utf8Length(cp);
            if (byteCount + cpBytes > MAX_REASON_BYTES) {
                break;
            }
            byteCount += cpBytes;
            charIndex += Character.charCount(cp);
        }

        String truncated = reason.substring(0, charIndex);
        return new CloseReason(closeCode, truncated.isEmpty() ? null : truncated);
    }

    private static int utf8Length(int cp) {
        if (cp <= 0x7F) return 1;
        if (cp <= 0x7FF) return 2;
        if (cp <= 0xFFFF) return 3;
        return 4;
    }
}
