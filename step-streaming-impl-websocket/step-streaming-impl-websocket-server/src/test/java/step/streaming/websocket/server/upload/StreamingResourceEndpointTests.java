package step.streaming.websocket.server.upload;

import jakarta.websocket.server.ServerEndpointConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.client.download.WebsocketDownload;
import step.streaming.client.download.WebsocketDownloadClient;
import step.streaming.client.upload.StreamingUploadSession;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.data.CheckpointingOutputStream;
import step.streaming.data.ClampedReadInputStream;
import step.streaming.data.MD5CalculatingInputStream;
import step.streaming.data.MD5CalculatingOutputStream;
import step.streaming.server.DefaultStreamingResourceManager;
import step.streaming.server.URITemplateBasedReferenceProducer;
import step.streaming.server.test.InMemoryCatalogBackend;
import step.streaming.server.test.TestingStorageBackend;
import step.streaming.websocket.client.upload.WebsocketUploadClient;
import step.streaming.websocket.client.upload.WebsocketUploadProvider;
import step.streaming.websocket.client.upload.WebsocketUploadSession;
import step.streaming.websocket.server.DefaultWebsocketServerEndpointSessionsHandler;
import step.streaming.websocket.server.WebsocketDownloadEndpoint;
import step.streaming.websocket.server.WebsocketServerEndpointSessionsHandler;
import step.streaming.websocket.server.WebsocketUploadEndpoint;
import step.streaming.websocket.test.TestingWebsocketServer;
import step.streaming.websocket.test.TricklingDelegatingInputStream;
import step.streaming.websocket.test.TricklingRandomBytesInputStream;

import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static step.streaming.common.StreamingResourceMetadata.CommonMimeTypes.APPLICATION_OCTET_STREAM;
import static step.streaming.common.StreamingResourceMetadata.CommonMimeTypes.TEXT_PLAIN;

public class StreamingResourceEndpointTests {
    private static final Logger logger = LoggerFactory.getLogger("TEST");
    private TestingStorageBackend storageBackend;
    private InMemoryCatalogBackend catalogBackend;
    private DefaultStreamingResourceManager manager;
    private WebsocketServerEndpointSessionsHandler sessionsHandler;
    private URITemplateBasedReferenceProducer referenceProducer;

    private static final String FAUST_ISO8859_CHECKSUM = "317c7a8df8c817c80bf079cfcbbc6686";
    private static final String FAUST_UTF8_CHECKSUM = "540441d13a31641d7775d91c46c94511";


    @Before
    public void setUp() throws IOException {
        sessionsHandler = new DefaultWebsocketServerEndpointSessionsHandler();
        storageBackend = new TestingStorageBackend(1000L, false);
        catalogBackend = new InMemoryCatalogBackend();
        referenceProducer = new URITemplateBasedReferenceProducer(null, WebsocketDownloadEndpoint.DEFAULT_ENDPOINT_URL, WebsocketDownloadEndpoint.DEFAULT_PARAMETER_NAME);
        manager = new DefaultStreamingResourceManager(catalogBackend, storageBackend,
                referenceProducer,
                null
        );
    }

    @After
    public void tearDown() throws Exception {
        Thread.sleep(1000);
        sessionsHandler.shutdown();
        storageBackend.cleanup();
    }

    private ServerEndpointConfig uploadConfig() {
        return ServerEndpointConfig.Builder.create(WebsocketUploadEndpoint.class, WebsocketUploadEndpoint.DEFAULT_ENDPOINT_URL)
                .configurator(new ServerEndpointConfig.Configurator() {
                    @Override
                    public <T> T getEndpointInstance(Class<T> endpointClass) throws InstantiationException {
                        return endpointClass.cast(new WebsocketUploadEndpoint(manager, sessionsHandler));
                    }
                })
                .build();
    }

    private ServerEndpointConfig downloadConfig() {
        return ServerEndpointConfig.Builder.create(WebsocketDownloadEndpoint.class, WebsocketDownloadEndpoint.DEFAULT_ENDPOINT_URL)
                .configurator(new ServerEndpointConfig.Configurator() {
                    @Override
                    public <T> T getEndpointInstance(Class<T> endpointClass) throws InstantiationException {
                        return endpointClass.cast(new WebsocketDownloadEndpoint(manager, sessionsHandler, WebsocketDownloadEndpoint.DEFAULT_PARAMETER_NAME));
                    }
                })
                .build();
    }

    @Test
    public void testLowLevelUploadFollowedByOneShotDownload() throws Exception {
        long DATA_SIZE = 500_000L;
        TestingWebsocketServer server = new TestingWebsocketServer().withEndpointConfigs(uploadConfig(), downloadConfig()).start();
        referenceProducer.setBaseUri(server.getURI());
        URI uploadUri = server.getURI(WebsocketUploadEndpoint.DEFAULT_ENDPOINT_URL);

        StreamingResourceMetadata metadata = new StreamingResourceMetadata("test.txt", TEXT_PLAIN, true);
        WebsocketUploadSession upload = new WebsocketUploadSession(metadata, null);
        // TODO: timeout
        //WebsocketUploadClient uploadClient = new WebsocketUploadClient(uploadUri, upload, ContainerProvider.getWebSocketContainer(), 200, 10);
        WebsocketUploadClient uploadClient = new WebsocketUploadClient(uploadUri, upload);
        InputStream data = new TricklingRandomBytesInputStream(DATA_SIZE, 5, TimeUnit.SECONDS);

        WebsocketDownloadClient downloadClient = new WebsocketDownloadClient(upload.getReference().getUri());
        //downloadClient.registerStatusListener(status -> System.err.println("download client status: " + status));
        upload.registerStatusListener(status -> {
            logger.info("upload status: {}", status);
        });
        // TODO: IO errors
        //uploadClient.performUpload(new FailingInputStream(data, 30, true));
        uploadClient.performUpload(data);

        Assert.assertEquals(DATA_SIZE, upload.getCurrentStatus().getCurrentSize());
        Assert.assertEquals(StreamingResourceTransferStatus.COMPLETED, upload.getCurrentStatus().getTransferStatus());

        Assert.assertEquals(DATA_SIZE, downloadClient.requestChunkTransfer(0, DATA_SIZE, OutputStream.nullOutputStream()).get().longValue());
        downloadClient.close();
        // give the system a few ms to finalize sessions
        Thread.sleep(50);
        sessionsHandler.shutdown();
        server.stop();
    }

    private static class RandomBytesProducer {
        File file;
        final InputStream input;
        final MD5CalculatingOutputStream output;
        String checksum;

        public RandomBytesProducer(long bytesCount, long duration, TimeUnit durationUnit) throws Exception {
            file = File.createTempFile("streaming-upload-test", ".tmp");
            file.deleteOnExit();
            input = new TricklingRandomBytesInputStream(bytesCount, duration, durationUnit);
            output = new MD5CalculatingOutputStream(new FileOutputStream(file));
        }

        // synchronous and blocking
        public long produce() throws Exception {
            long transferred = input.transferTo(output);
            input.close();
            output.close();
            checksum = output.getChecksum();
            return transferred;
        }
    }

    private static class FileBytesProducer {
        File file;
        final InputStream input;
        final MD5CalculatingOutputStream output;
        String checksum;

        public FileBytesProducer(File sourceFile, long duration, TimeUnit durationUnit) throws Exception {
            file = File.createTempFile("streaming-upload-test", ".tmp");
            file.deleteOnExit();
            input = new ClampedReadInputStream(new TricklingDelegatingInputStream(new FileInputStream(sourceFile), sourceFile.length(), duration, durationUnit));
            output = new MD5CalculatingOutputStream(new FileOutputStream(file));
        }

        // synchronous and blocking
        public long produce() throws Exception {
            long transferred = input.transferTo(output);
            input.close();
            output.close();
            checksum = output.getChecksum();
            return transferred;
        }
    }


    @Test
    public void testHighLevelUploadWithSimultaneousDownloadsRandomData() throws Exception {
        long DATA_SIZE = 200_000_000L;
        RandomBytesProducer randomBytesProducer = new RandomBytesProducer(DATA_SIZE, 5, TimeUnit.SECONDS);

        TestingWebsocketServer server = new TestingWebsocketServer().withEndpointConfigs(uploadConfig(), downloadConfig()).start();
        URI uploadUri = server.getURI(WebsocketUploadEndpoint.DEFAULT_ENDPOINT_URL);
        referenceProducer.setBaseUri(server.getURI());

        WebsocketUploadProvider provider = new WebsocketUploadProvider(uploadUri);
        StreamingUploadSession upload = provider.startLiveBinaryFileUpload(randomBytesProducer.file, new StreamingResourceMetadata("test.bin", APPLICATION_OCTET_STREAM, true));
        WebsocketDownload download = new WebsocketDownload(upload.getReference());
        AtomicReference<String> downloadChecksum = new AtomicReference<>();
        Thread downloadThread = new Thread(() -> {
            try (
                    CheckpointingOutputStream out = new CheckpointingOutputStream(OutputStream.nullOutputStream(), 500, read ->
                            logger.info("Download status: {} bytes currently transferred", read));
                    MD5CalculatingInputStream downloadStream = new MD5CalculatingInputStream(download.getInputStream());
            ) {
                long downloadSize = downloadStream.transferTo(out);
                downloadStream.close();
                downloadChecksum.set(downloadStream.getChecksum());
                logger.info("Final download size={}, checksum={}", downloadSize, downloadChecksum.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        downloadThread.start();
        Assert.assertEquals(DATA_SIZE, randomBytesProducer.produce());
        Assert.assertEquals(DATA_SIZE, randomBytesProducer.file.length());
        upload.signalEndOfInput();
        logger.info("UPLOAD FINAL STATUS: {}", upload.getFinalStatusFuture().get());
        upload.close(); // optional
        downloadThread.join();
        Assert.assertEquals(randomBytesProducer.checksum, downloadChecksum.get());
        // do another transfer, this time of the finished file
        MD5CalculatingOutputStream md5Out = new MD5CalculatingOutputStream(OutputStream.nullOutputStream());
        long again = download.getInputStream().transferTo(md5Out);
        Assert.assertEquals(DATA_SIZE, again);
        md5Out.close();
        Assert.assertEquals(randomBytesProducer.checksum, md5Out.getChecksum());
        download.close();
        Thread.sleep(50);
        sessionsHandler.shutdown();
        server.stop();
    }

    @Test
    public void testHighLevelUploadWithSimultaneousDownloadsWithTextConversion() throws Exception {
        URL url = Thread.currentThread().getContextClassLoader().getResource("Faust-8859-1.txt");
        File sourceFile = new File(url.toURI());
        long INPUT_DATA_SIZE = 10171; // in ISO-8859-1 format
        long OUTPUT_DATA_SIZE = 10324; // in UTF-8 format, i.e. what is expected to arrive server-side
        Long OUTPUT_LINES = 296L;
        FileBytesProducer isoBytesProducer = new FileBytesProducer(sourceFile, 5, TimeUnit.SECONDS);

        TestingWebsocketServer server = new TestingWebsocketServer().withEndpointConfigs(uploadConfig(), downloadConfig()).start();
        URI uploadUri = server.getURI(WebsocketUploadEndpoint.DEFAULT_ENDPOINT_URL);
        referenceProducer.setBaseUri(server.getURI());

        WebsocketUploadProvider provider = new WebsocketUploadProvider(uploadUri);
        // This will transcode the file to UTF-8 on upload (on the fly)
        StreamingUploadSession upload = provider.startLiveTextFileUpload(isoBytesProducer.file, new StreamingResourceMetadata("faust.txt", TEXT_PLAIN, true), StandardCharsets.ISO_8859_1);
        WebsocketDownload download = new WebsocketDownload(upload.getReference());
        AtomicReference<String> downloadChecksum = new AtomicReference<>();
        Thread downloadThread = new Thread(() -> {
            try (
                    CheckpointingOutputStream out = new CheckpointingOutputStream(OutputStream.nullOutputStream(), 500, read ->
                            logger.info("Download status: {} bytes currently transferred", read));
                    MD5CalculatingInputStream downloadStream = new MD5CalculatingInputStream(download.getInputStream());
            ) {
                long downloadSize = downloadStream.transferTo(out);
                downloadStream.close();
                downloadChecksum.set(downloadStream.getChecksum());
                logger.info("Final download size={}, checksum={}", downloadSize, downloadChecksum.get());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        downloadThread.start();
        Assert.assertEquals(INPUT_DATA_SIZE, isoBytesProducer.produce());
        Assert.assertEquals(INPUT_DATA_SIZE, isoBytesProducer.file.length());
        upload.signalEndOfInput();
        StreamingResourceStatus finalUploadStatus = upload.getFinalStatusFuture().join();
        logger.info("UPLOAD FINAL STATUS: {}", finalUploadStatus);
        Assert.assertEquals(OUTPUT_LINES, finalUploadStatus.getNumberOfLines());
        Assert.assertEquals(OUTPUT_DATA_SIZE, finalUploadStatus.getCurrentSize());
        Assert.assertEquals(StreamingResourceTransferStatus.COMPLETED, finalUploadStatus.getTransferStatus());
        upload.close(); // optional
        downloadThread.join();
        Assert.assertEquals(FAUST_ISO8859_CHECKSUM, isoBytesProducer.checksum);
        Assert.assertEquals(FAUST_UTF8_CHECKSUM, downloadChecksum.get());

        // do another transfer, this time of the finished file
        MD5CalculatingOutputStream md5Out = new MD5CalculatingOutputStream(OutputStream.nullOutputStream());
        long again = download.getInputStream().transferTo(md5Out);
        Assert.assertEquals(OUTPUT_DATA_SIZE, again);
        md5Out.close();
        Assert.assertEquals(FAUST_UTF8_CHECKSUM, md5Out.getChecksum());
        download.close();
        Thread.sleep(50);
        sessionsHandler.shutdown();
        server.stop();
    }

    @Test
    public void testErrorScenarios() throws Exception {
        TestingWebsocketServer server = new TestingWebsocketServer().withEndpointConfigs(uploadConfig(), downloadConfig()).start();
        URI uploadUri = server.getURI(WebsocketUploadEndpoint.DEFAULT_ENDPOINT_URL);
        referenceProducer.setBaseUri(server.getURI());

        File sourceFile = new File(Thread.currentThread().getContextClassLoader().getResource("Faust-8859-1.txt").toURI());
        FileBytesProducer uploadProducer = new FileBytesProducer(sourceFile, 15, TimeUnit.SECONDS);

        WebsocketUploadProvider provider = new WebsocketUploadProvider(uploadUri);
        StreamingUploadSession upload = provider.startLiveTextFileUpload(uploadProducer.file, new StreamingResourceMetadata("faust.txt", TEXT_PLAIN, true), StandardCharsets.ISO_8859_1);
        Thread producerThread = new Thread(() -> {
            try {
                uploadProducer.produce();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        producerThread.start();
        Thread.sleep(2000);
        // interrupt upload by closing session, but without setting it to completed
        try {
            upload.close();
            Assert.fail("Expected exception to be thrown");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("Upload session was closed before input was signalled to be complete"));
        }
        try {
            upload.getFinalStatusFuture().join();
            Assert.fail("Expected exception to be thrown");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("Upload session was closed before input was signalled to be complete"));
        }
    }

    @Test
    public void testLineBasedDownload() throws Exception {
        TestingWebsocketServer server = new TestingWebsocketServer().withEndpointConfigs(uploadConfig(), downloadConfig()).start();
        URI uploadUri = server.getURI(WebsocketUploadEndpoint.DEFAULT_ENDPOINT_URL);
        referenceProducer.setBaseUri(server.getURI());

        File sourceFile = new File(Thread.currentThread().getContextClassLoader().getResource("Faust-8859-1.txt").toURI());
        FileBytesProducer uploadProducer = new FileBytesProducer(sourceFile, 15, TimeUnit.SECONDS);

        WebsocketUploadProvider provider = new WebsocketUploadProvider(uploadUri);
        StreamingUploadSession upload = provider.startLiveTextFileUpload(uploadProducer.file, new StreamingResourceMetadata("faust.txt", TEXT_PLAIN, true), StandardCharsets.ISO_8859_1);

        WebsocketDownloadClient downloadClient = new WebsocketDownloadClient(upload.getReference().getUri());

        MD5CalculatingOutputStream md5Out = new MD5CalculatingOutputStream(OutputStream.nullOutputStream());
        Thread downloadThread = new Thread(() -> {
            CompletableFuture<Boolean> completed = new CompletableFuture<>();
            AtomicLong linesReceived = new AtomicLong(0);
            AtomicReference<StreamingResourceStatus> status = new AtomicReference<>();
            // we'll directly request newly available lines when the server says they're ready
            downloadClient.registerStatusListener(serverSideStatus -> {
                status.set(serverSideStatus);
                long linesToFetch = status.get().getNumberOfLines() - linesReceived.get();
                if (status.get().getTransferStatus().equals(StreamingResourceTransferStatus.FAILED)) {
                    completed.completeExceptionally(new IllegalStateException("" + StreamingResourceTransferStatus.FAILED));
                }
                if (linesToFetch > 0) {
                    try {
                        downloadClient.requestTextLines(linesReceived.get(), linesToFetch, lines -> {
                            linesReceived.addAndGet(lines.size());
                            for (String line : lines) {
                                try {
                                    md5Out.write(line.getBytes(StandardCharsets.UTF_8));
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else if (status.get().getTransferStatus().equals(StreamingResourceTransferStatus.COMPLETED)) {
                    completed.complete(true);
                }
            });
            try {
                completed.get();
                if (linesReceived.get() != 296) {
                    throw new RuntimeException("Expected 296 lines, got " + linesReceived.get());
                }
                md5Out.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        downloadThread.start();

        // synchronous and blocking
        uploadProducer.produce();
        upload.signalEndOfInput();
        downloadThread.join();
        downloadClient.close();

        Thread.sleep(50);
        sessionsHandler.shutdown();
        server.stop();

        // we retrieved the data using line-based access, but this should be exactly equivalent to the raw file
        Assert.assertEquals(FAUST_UTF8_CHECKSUM, md5Out.getChecksum());
    }

    @Test
    public void testFailedDownload() throws Exception {
        TestingWebsocketServer server = new TestingWebsocketServer().withEndpointConfigs(uploadConfig(), downloadConfig()).start();
        URI uploadUri = server.getURI(WebsocketUploadEndpoint.DEFAULT_ENDPOINT_URL);
        referenceProducer.setBaseUri(server.getURI());

        WebsocketUploadProvider uploadProvider = new WebsocketUploadProvider(uploadUri);

        testFailedDownloadWithInput(uploadProvider, "Failing");
        testFailedDownloadWithInput(uploadProvider, "Failing\n");
    }

    private static void testFailedDownloadWithInput(WebsocketUploadProvider uploadProvider, String input) throws IOException, InterruptedException {
        File dataFile = Files.createTempFile("step-streaming-test-", ".txt").toFile();
        try {
            var uploadSession = uploadProvider.startLiveTextFileUpload(dataFile, new StreamingResourceMetadata("dummy.txt", TEXT_PLAIN, true), StandardCharsets.UTF_8);
            var reference = uploadSession.getReference();
            Files.writeString(dataFile.toPath(), input);
            Thread.sleep(100);
            // This will cause a failure because the file was not signalled to be complete
            try {
                uploadSession.close();
            } catch (IOException expected) {

            }
            var download = new WebsocketDownload(reference);
            ByteArrayOutputStream data = new ByteArrayOutputStream();
            try (InputStream in = download.getInputStream()) {
                in.transferTo(data);
            }
            // We expect numberOfLines==1, regardless of whether input was terminated by LB or not
            Assert.assertEquals(new StreamingResourceStatus(StreamingResourceTransferStatus.FAILED, input.length(), 1L), download.getCurrentStatus());
            String downloaded = data.toString();
            Assert.assertEquals(input, downloaded);
        } finally {
            Files.deleteIfExists(dataFile.toPath());
        }
    }
}
