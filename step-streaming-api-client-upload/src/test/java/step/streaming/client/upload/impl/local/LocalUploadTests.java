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
            } catch (Exception ignored) {}
            Files.deleteIfExists(liveFile.toPath());
        }
    }

    private class LineProducer extends Thread{
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
