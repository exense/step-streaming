package step.streaming.server.data;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LinebreakDetectingOutputStreamTests {

    @Test
    public void testLinebreakPositionsSingleBytes() throws IOException {
        ByteArrayOutputStream target = new ByteArrayOutputStream();
        List<Long> detectedPositions = new ArrayList<>();

        try (LinebreakDetectingOutputStream stream =
                     new LinebreakDetectingOutputStream(target, detectedPositions::add)) {

            String input = "\nabc\ndef\nghi";
            byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
            for (byte b : bytes) {
                stream.write(b);
            }
        }

        // Expect \n at positions 0, 4, and 8
        assertEquals(List.of(0L, 4L, 8L), detectedPositions);
        assertEquals("\nabc\ndef\nghi", target.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testLinebreakPositionsBulkWrite() throws IOException {
        ByteArrayOutputStream target = new ByteArrayOutputStream();
        List<Long> detectedPositions = new ArrayList<>();

        byte[] data = "line1\nline2\nno linebreak".getBytes(StandardCharsets.UTF_8);

        try (LinebreakDetectingOutputStream stream =
                     new LinebreakDetectingOutputStream(target, detectedPositions::add)) {
            stream.write(data, 0, data.length);
        }

        assertEquals(List.of(5L, 11L), detectedPositions);
        assertEquals("line1\nline2\nno linebreak", target.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testMixedWrites() throws IOException {
        ByteArrayOutputStream target = new ByteArrayOutputStream();
        List<Long> detectedPositions = new ArrayList<>();

        try (LinebreakDetectingOutputStream stream =
                     new LinebreakDetectingOutputStream(target, detectedPositions::add)) {

            stream.write("abc\n".getBytes(StandardCharsets.UTF_8));
            stream.write('x');
            stream.write('\n');
            stream.write("tail".getBytes(StandardCharsets.UTF_8));
        }

        // \n at pos 3 (abc\n) and 5 (after 'x')
        assertEquals(List.of(3L, 5L), detectedPositions);
        assertEquals("abc\nx\ntail", target.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testNoLinebreaks() throws IOException {
        ByteArrayOutputStream target = new ByteArrayOutputStream();
        List<Long> detectedPositions = new ArrayList<>();

        try (LinebreakDetectingOutputStream stream =
                     new LinebreakDetectingOutputStream(target, detectedPositions::add)) {
            stream.write("abcdefg".getBytes(StandardCharsets.UTF_8));
        }

        assertTrue(detectedPositions.isEmpty());
        assertEquals("abcdefg", target.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testMultipleLinebreaksInOneWrite() throws IOException {
        ByteArrayOutputStream target = new ByteArrayOutputStream();
        List<Long> detectedPositions = new ArrayList<>();

        String input = "\n\n\n"; // 3 linebreaks
        byte[] data = input.getBytes(StandardCharsets.UTF_8);

        try (LinebreakDetectingOutputStream stream =
                     new LinebreakDetectingOutputStream(target, detectedPositions::add)) {
            stream.write(data);
        }

        assertEquals(List.of(0L, 1L, 2L), detectedPositions);
        assertEquals(input, target.toString(StandardCharsets.UTF_8));
    }

    @Test
    public void testFlushAndCloseDelegation() throws IOException {
        TestableOutputStream target = new TestableOutputStream();
        List<Long> dummy = new ArrayList<>();

        LinebreakDetectingOutputStream stream = new LinebreakDetectingOutputStream(target, dummy::add);
        stream.flush();
        stream.close();

        assertTrue(dummy.isEmpty());
        assertTrue(target.flushed);
        assertTrue(target.closed);
    }

    private static class TestableOutputStream extends ByteArrayOutputStream {
        boolean flushed = false;
        boolean closed = false;

        @Override
        public void flush() throws IOException {
            flushed = true;
            super.flush();
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }
}
