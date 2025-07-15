package step.streaming.websocket.test;

import org.junit.Assert;
import org.junit.Test;
import step.streaming.data.CheckpointingOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

@SuppressWarnings("resource")
public class TricklingRandomBytesInputStreamTest {

    @Test
    public void testQuick() throws Exception {
        InputStream in = new TricklingRandomBytesInputStream(100, 500, TimeUnit.MILLISECONDS);
        Assert.assertEquals(100, in.transferTo(OutputStream.nullOutputStream()));
    }

    @Test
    public void testSlow() throws Exception {
        long start = System.currentTimeMillis();
        InputStream in = new TricklingRandomBytesInputStream(1024 * 1024, 5, TimeUnit.SECONDS);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        CheckpointingOutputStream out2 = new CheckpointingOutputStream(out, 1000, System.err::println);
        in.transferTo(out2);
        Assert.assertEquals(1024 * 1024, out.toByteArray().length);
        long duration = System.currentTimeMillis() - start;
        // give it a little leeway
        Assert.assertTrue(duration > 4500 && duration < 5500);
    }
}
