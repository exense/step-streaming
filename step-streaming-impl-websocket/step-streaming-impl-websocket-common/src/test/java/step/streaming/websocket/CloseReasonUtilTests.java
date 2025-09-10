package step.streaming.websocket;

import jakarta.websocket.CloseReason;
import org.junit.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.Assert.*;

public class CloseReasonUtilTests {

    @Test(expected = IllegalArgumentException.class)
    public void nullCodeThrows() {
        CloseReasonUtil.makeSafeCloseReason(null, "reason");
    }

    @Test
    public void asciiUnderLimitIsUnchanged() {
        String reason = "short reason";
        CloseReason cr = CloseReasonUtil.makeSafeCloseReason(dummyCode(), reason);
        assertEquals(reason, cr.getReasonPhrase());
    }

    @Test
    public void longAsciiIsTruncated() {
        String longReason = repeat("a", 200); // 200 bytes in ASCII
        CloseReason cr = CloseReasonUtil.makeSafeCloseReason(dummyCode(), longReason);
        assertTrue(cr.getReasonPhrase().length() < longReason.length());
        assertEquals(CloseReasonUtil.MAX_REASON_BYTES, cr.getReasonPhrase().getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    public void emojiIsHandledAsWholeCharacter() {
        String emoji = "\uD83D\uDE80"; // 🚀 (4 bytes in UTF-8)
        String longReason = repeat("a", 120) + emoji + "zzz"; // push over 123
        CloseReason cr = CloseReasonUtil.makeSafeCloseReason(dummyCode(), longReason);
        String phrase = cr.getReasonPhrase();
        // Should not cut the surrogate pair (🚀) -- in fact, it will remove the entire emoji.
        assertFalse(phrase.endsWith("\uD83D"));
        assertEquals(120, phrase.getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    public void emojiBarelyfits() {
        String emoji = "\uD83D\uDE80"; // 🚀 (4 bytes in UTF-8)
        String longReason = repeat("a", 119) + emoji + "zzz"; // push over 123
        CloseReason cr = CloseReasonUtil.makeSafeCloseReason(dummyCode(), longReason);
        String phrase = cr.getReasonPhrase();
        assertTrue(phrase.endsWith("\uDE80")); // should end exactly with the emoji.
        //System.err.println(phrase);
        assertEquals(123, phrase.getBytes(StandardCharsets.UTF_8).length);
    }

    // --- helpers ---

    private static String repeat(String s, int count) {
        StringBuilder sb = new StringBuilder(s.length() * count);
        for (int i = 0; i < count; i++) sb.append(s);
        return sb.toString();
    }

    private static CloseReason.CloseCode dummyCode() {
        // Example: use one of the standard codes
        return CloseReason.CloseCodes.NORMAL_CLOSURE;
    }
}
