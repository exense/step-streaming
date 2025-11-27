package step.streaming.client.upload.impl.local;

import org.junit.Assert;
import org.junit.Test;
import step.streaming.client.upload.StreamingUpload;
import step.streaming.client.upload.StreamingUploads;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;

public class LocalUploadTests {

    @Test
    public void testDiscardingUpload() throws Exception {
        StreamingUploads uploads = new StreamingUploads(new DiscardingStreamingUploadProvider());
        File liveFile = Files.createTempFile("streaming-upload-test-", ".txt").toFile();
        LineProducer producer = new LineProducer(liveFile, 10, 200);
        try {
            StreamingUpload upload = uploads.startTextFileUpload(liveFile);
            producer.start();
            producer.join();
            Assert.assertEquals(new StreamingResourceStatus(StreamingResourceTransferStatus.COMPLETED, 141, 10L), upload.complete(Duration.ofSeconds(1)));
        } finally {
            try {
                producer.stop();
            } catch (Exception ignored) {
            }
            Files.deleteIfExists(liveFile.toPath());
        }
    }

    @Test
    public void testDirectoryUpload() throws Exception {
        File tmpDir = Files.createTempDirectory("streaming-upload-test-").toFile();
        StreamingUploads uploads = new StreamingUploads(new LocalDirectoryBackedStreamingUploadProvider(Executors.newSingleThreadExecutor(), tmpDir));
        File liveFile = Files.createTempFile("streaming-upload-test-", ".txt").toFile();
        LineProducer producer = new LineProducer(liveFile, 10, 200);
        try {
            StreamingUpload upload = uploads.startTextFileUpload(liveFile);
            producer.start();
            producer.join();
            Assert.assertEquals(new StreamingResourceStatus(StreamingResourceTransferStatus.COMPLETED, 141, 10L), upload.complete(Duration.ofSeconds(1)));
            Assert.assertEquals(1, Objects.requireNonNull(tmpDir.listFiles()).length);
            File uploaded = Objects.requireNonNull(tmpDir.listFiles())[0];
            Assert.assertTrue(uploaded.getName().endsWith(liveFile.getName()));
            Assert.assertTrue(uploaded.delete());
        } finally {
            try {
                producer.stop();
            } catch (Exception ignored) {
            }
            Files.deleteIfExists(liveFile.toPath());
            Files.deleteIfExists(tmpDir.toPath());
        }
    }


    private static class LineProducer extends Thread {
        private final File outputFile;
        private final int numberOfLines;
        private final long sleepBetween;

        LineProducer(File outputFile, int numberOfLines, long sleepBetweenMs) {
            this.outputFile = outputFile;
            this.numberOfLines = numberOfLines;
            this.sleepBetween = sleepBetweenMs;
        }

        @Override
        public void run() {
            try {
                for (int l = 1; l <= numberOfLines; l++) {
                    Files.writeString(outputFile.toPath(), "Line number " + l + "\n", StandardOpenOption.APPEND);
                    Thread.sleep(sleepBetween);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
