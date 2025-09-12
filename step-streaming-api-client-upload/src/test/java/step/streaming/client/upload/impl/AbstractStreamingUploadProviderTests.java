package step.streaming.client.upload.impl;

import org.junit.Assert;
import org.junit.Test;
import step.streaming.client.upload.StreamingUpload;
import step.streaming.client.upload.StreamingUploadProvider;
import step.streaming.client.upload.StreamingUploads;
import step.streaming.client.upload.impl.local.DiscardingStreamingUploadProvider;
import step.streaming.client.upload.impl.local.LocalStreamingUploadSession;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.data.EndOfInputSignal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AbstractStreamingUploadProviderTests {

    @Test
    public void testIllegalArguments() throws Exception {
        File dummyFile = Files.createTempFile("streaming-tests", ".bin").toFile();
        try {
            StreamingUploadProvider provider = new DiscardingStreamingUploadProvider();
            var metadata = new StreamingResourceMetadata("file.bin", StreamingResourceMetadata.CommonMimeTypes.TEXT_PLAIN, true);
            Exception e = Assert.assertThrows(NullPointerException.class, () -> provider.startLiveTextFileUpload(dummyFile, metadata, null));
            Assert.assertEquals("charset is required for text files", e.getMessage());

            e = Assert.assertThrows(IllegalArgumentException.class, () -> provider.startLiveBinaryFileUpload(dummyFile, metadata));
            Assert.assertEquals("metadata indicates line-access support, which is not allowed for binary files. Use startLiveTextFileUpload instead.", e.getMessage());
        } finally {
            Files.deleteIfExists(dummyFile.toPath());
        }
    }


    @Test
    public void testClosing() throws Exception {
        // The tests both that ongoing uploads are correctly cancelled, and the edge case for which the semaphore
        // is required, where events happen in the following order:
        // 1. upload start initiated, 2. close initiated, 3. upload start ending
        // The implementation is much simpler than the test ;-)
        CompletableFuture<Void> uploadStarted = new CompletableFuture<>();
        StreamingUploads uploads = new StreamingUploads(new DiscardingStreamingUploadProvider() {
            @Override
            protected LocalStreamingUploadSession startLiveFileUpload(InputStream sourceInputStream,
                                                                      StreamingResourceMetadata metadata,
                                                                      EndOfInputSignal endOfInputSignal) {
                var session = super.startLiveFileUpload(sourceInputStream, metadata, endOfInputSignal);
                try {
                    // Here, we're "inside" the semaphore holding a permit
                    uploadStarted.complete(null);
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {}
                return session;
            }
        });
        File dummyFile = Files.createTempFile("streaming-upload-test-", ".txt").toFile();
        try {
            CompletableFuture<StreamingUpload> uploadRef = new CompletableFuture<>();
            new Thread(() -> {
                try {
                    uploadRef.complete(uploads.startTextFileUpload(dummyFile));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).start();
            // wait for upload to have started
            uploadStarted.join();
            // double-check that it's still running
            Assert.assertFalse(uploadRef.isDone());
            // This is the edge case: closing while an upload is being started
            uploads.close();
            // The close() must have waited for the upload to finish initializing, only to cancel it immediately
            Assert.assertTrue(uploadRef.get(500, TimeUnit.MILLISECONDS).getSession().getFinalStatusFuture().isDone());
            uploadRef.get().getSession().getFinalStatusFuture().handle((streamingResourceStatus, throwable) -> {
                // we expect an exception indicating that the upload was canceled
                Assert.assertNull(streamingResourceStatus);
                Assert.assertTrue(throwable instanceof IOException && throwable.getMessage().contains("Upload session was closed before input was signalled to be complete"));
                return null;
            });

            // One more much simpler test
            try {
                uploads.startTextFileUpload(dummyFile);
                Assert.fail("Exception expected");
            } catch (IOException e) {
                Assert.assertEquals("Upload provider is closed, not accepting new uploads", e.getMessage());
            }
        } finally {
            Files.deleteIfExists(dummyFile.toPath());
        }
    }

}
