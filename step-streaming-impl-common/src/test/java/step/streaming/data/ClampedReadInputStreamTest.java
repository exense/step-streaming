package step.streaming.data;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClampedReadInputStreamTest {

    private static byte[] createData(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) data[i] = (byte) i;
        return data;
    }

    @Test
    public void testReadClampsBufferSize() throws IOException {
        byte[] original = createData(100);
        InputStream base = new ByteArrayInputStream(original);
        ClampedReadInputStream clamped = new ClampedReadInputStream(base, 10);

        byte[] buffer = new byte[50]; // deliberately larger than clamp
        int read = clamped.read(buffer, 0, buffer.length);

        assertEquals(10, read); // should clamp to 10 bytes
        for (int i = 0; i < 10; i++) {
            assertEquals((byte) i, buffer[i]);
        }
    }

    @Test
    public void testReadAllWithClamping() throws IOException {
        byte[] original = createData(25);
        InputStream base = new ByteArrayInputStream(original);
        ClampedReadInputStream clamped = new ClampedReadInputStream(base, 8);

        byte[] buffer = new byte[10]; // buffer larger than clamp
        int total = 0;
        int read;

        while ((read = clamped.read(buffer)) != -1) {
            assertTrue("Should never read more than 8 bytes", read <= 8);
            for (int i = 0; i < read; i++) {
                assertEquals(original[total + i], buffer[i]);
            }
            total += read;
        }

        assertEquals(25, total);
    }

    @Test
    public void testSingleByteReadUnchanged() throws IOException {
        byte[] original = {42, 43, 44};
        InputStream base = new ByteArrayInputStream(original);
        ClampedReadInputStream clamped = new ClampedReadInputStream(base, 1);

        assertEquals(42, clamped.read());
        assertEquals(43, clamped.read());
        assertEquals(44, clamped.read());
        assertEquals(-1, clamped.read());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidClampSizeThrows() {
        new ClampedReadInputStream(new ByteArrayInputStream(new byte[0]), 0);
    }

    @SuppressWarnings("resource")
    @Test(expected = NullPointerException.class)
    public void testNullDelegateThrows() {
        new ClampedReadInputStream(null, 10);
    }
}
