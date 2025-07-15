package step.streaming.data;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class UTF8TranscodingTextInputStreamTests {

    private String readAll(InputStream in) throws Exception {
        byte[] buffer = new byte[1024];
        int n;
        StringBuilder sb = new StringBuilder();
        while ((n = in.read(buffer)) != -1) {
            sb.append(new String(buffer, 0, n, StandardCharsets.UTF_8));
        }
        return sb.toString();
    }

    @Test
    public void testLatin1ToUtf8() throws Exception {
        // "Café" in ISO-8859-1
        byte[] latin1 = {(byte) 'C', (byte) 'a', (byte) 'f', (byte) 0xE9};
        InputStream in = new UTF8TranscodingTextInputStream(new ByteArrayInputStream(latin1), StandardCharsets.ISO_8859_1);
        String result = readAll(in);
        assertEquals("Café", result);
    }

    @Test
    public void testUtf16ToUtf8() throws Exception {
        // "こんにちは" (Hello in Japanese) in UTF-16LE
        byte[] utf16le = {
                (byte) 0x53, (byte) 0x30, (byte) 0x93, (byte) 0x30, (byte) 0x6B, (byte) 0x30,
                (byte) 0x61, (byte) 0x30, (byte) 0x6F, (byte) 0x30
        };
        InputStream in = new UTF8TranscodingTextInputStream(new ByteArrayInputStream(utf16le), StandardCharsets.UTF_16LE);
        String result = readAll(in);
        assertEquals("こんにちは", result);
    }

    @Test
    public void testEmptyInput() throws Exception {
        InputStream in = new UTF8TranscodingTextInputStream(new ByteArrayInputStream(new byte[0]), StandardCharsets.UTF_8);
        String result = readAll(in);
        assertEquals("", result);
    }

    // The following two tests use the same input bytes, but interpreted as different encodings.
    // Expected behavior is documented in the test.
    @Test
    public void testIso88591DecodingPreservesBytes() throws Exception {
        // This byte sequence is invalid UTF-8, but valid ISO-8859-1 (Latin-1),
        // so it should decode to "Ã(" without any replacement characters.
        byte[] bytes = {(byte) 0xC3, (byte) 0x28};
        InputStream in = new UTF8TranscodingTextInputStream(new ByteArrayInputStream(bytes), StandardCharsets.ISO_8859_1);
        String result = readAll(in);
        assertEquals("Ã(", result);
    }

    @Test
    public void testMalformedUtf8InputIsReplaced() throws Exception {
        // This byte sequence is malformed in UTF-8:
        // 0xC3 expects a continuation byte (0x80–0xBF), but 0x28 is '(' (not valid).
        // The decoder should replace the bad sequence with � and then decode the remaining byte.
        byte[] malformedUtf8 = {(byte) 0xC3, (byte) 0x28};

        InputStream in = new UTF8TranscodingTextInputStream(new ByteArrayInputStream(malformedUtf8), StandardCharsets.UTF_8);
        String result = readAll(in);

        // First byte is invalid sequence => replacement char
        // Second byte (0x28) is '(', which is valid ASCII
        assertEquals("�(", result);
    }



    @Test
    public void testUtf8WithBom() throws Exception {
        byte[] bomUtf8 = new byte[]{(byte) 0xEF, (byte) 0xBB, (byte) 0xBF, 'H', 'i'};
        InputStream in = new UTF8TranscodingTextInputStream(new ByteArrayInputStream(bomUtf8), StandardCharsets.UTF_8);
        String result = readAll(in);
        assertEquals("Hi", result);
    }

    @Test
    public void testUtf16LEWithBom() throws Exception {
        // UTF-16LE BOM + "Hi"
        byte[] bomUtf16LE = new byte[]{
                (byte) 0xFF, (byte) 0xFE,  // BOM
                'H', 0x00, 'i', 0x00     // "H", "i"
        };
        InputStream in = new UTF8TranscodingTextInputStream(new ByteArrayInputStream(bomUtf16LE), StandardCharsets.UTF_16LE);
        String result = readAll(in);
        assertEquals("Hi", result);
    }

    @Test
    public void testUtf16LEWithConflictingBom() throws Exception {
        // UTF-16LE BOM + "Hi"
        byte[] bomUtf16LE = new byte[]{
                (byte) 0xFF, (byte) 0xFE,  // BOM
                'H', 0x00, 'i', 0x00     // "H", "i"
        };
        // Wrong encoding given as argument!
        InputStream in = new UTF8TranscodingTextInputStream(new ByteArrayInputStream(bomUtf16LE), StandardCharsets.UTF_16BE);
        try {
            readAll(in);
            fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertEquals("BOM indicates UTF-16LE, but declared charset is: UTF-16BE", e.getMessage());
        }
    }

    @Test
    public void testUtf16BEWithBom() throws Exception {
        // UTF-16BE BOM + "Hi"
        byte[] bomUtf16BE = new byte[]{
                (byte) 0xFE, (byte) 0xFF,  // BOM
                0x00, 'H', 0x00, 'i'     // "H", "i"
        };
        InputStream in = new UTF8TranscodingTextInputStream(new ByteArrayInputStream(bomUtf16BE), StandardCharsets.UTF_16BE);
        String result = readAll(in);
        assertEquals("Hi", result);
    }


    @Test
    public void testBomBytesAreNotStrippedInIso88591() throws Exception {
        // 0xFF -> 'ÿ', 0xFE -> 'þ' in ISO-8859-1
        byte[] input = {(byte) 0xFF, (byte) 0xFE};

        // Declared charset is ISO-8859-1, so no BOM detection should occur
        InputStream in = new UTF8TranscodingTextInputStream(new ByteArrayInputStream(input), StandardCharsets.ISO_8859_1);
        String result = readAll(in);

        // Expect "ÿþ" (U+00FF U+00FE) transcoded to UTF-8
        assertEquals("ÿþ", result);
    }

    @Test
    public void testBomOnly() throws Exception {
        // pathological case: BOM only, nothing afterward -> 0-byte output.
        byte[] input = {(byte) 0xFF, (byte) 0xFE};

        InputStream in = new UTF8TranscodingTextInputStream(new ByteArrayInputStream(input), StandardCharsets.UTF_16);
        String result = readAll(in);

        assertEquals(0, result.length());
    }

}
