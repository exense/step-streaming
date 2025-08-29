package step.streaming.websocket.server.upload;

import jakarta.websocket.server.ServerEndpointConfig;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.client.download.WebsocketDownload;
import step.streaming.client.download.WebsocketDownloadClient;
import step.streaming.common.QuotaExceededException;
import step.streaming.client.upload.StreamingUpload;
import step.streaming.client.upload.StreamingUploadSession;
import step.streaming.client.upload.StreamingUploads;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.common.StreamingResourceUploadContexts;
import step.streaming.data.*;
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
import step.streaming.websocket.test.TestingResourceManager;
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

import static org.junit.Assert.*;
import static step.streaming.common.StreamingResourceMetadata.CommonMimeTypes.APPLICATION_OCTET_STREAM;
import static step.streaming.common.StreamingResourceMetadata.CommonMimeTypes.TEXT_PLAIN;

/* Important note:
On the build server (only), something extremely weird was happening when the setup and teardown methods
were annotated with @Before and @After: the teardown method would be run BEFORE the actual test methods
were completed, causing failing tests. I have no clue what the hell was happening there, and I couldn't
reproduce it locally. The attempt to fix it is to have each test explicitly call the setup and teardown
methods. It's cumbersome, but hopefully fixes the situation...
 */
public class StreamingResourceEndpointTests {
    private static final Logger logger = LoggerFactory.getLogger("UNITTEST");
    private TestingStorageBackend storageBackend;
    private InMemoryCatalogBackend catalogBackend;
    private TestingResourceManager manager;
    private WebsocketServerEndpointSessionsHandler sessionsHandler;
    private URITemplateBasedReferenceProducer referenceProducer;
    private TestingWebsocketServer server;
    private URI uploadUri;

    private static final String FAUST_ISO8859_CHECKSUM = "317c7a8df8c817c80bf079cfcbbc6686";
    private static final String FAUST_UTF8_CHECKSUM = "540441d13a31641d7775d91c46c94511";


    public void setUp() throws Exception {
        logger.info("setUp() starting");
        sessionsHandler = new DefaultWebsocketServerEndpointSessionsHandler();
        storageBackend = new TestingStorageBackend(1000L, false);
        catalogBackend = new InMemoryCatalogBackend();
        referenceProducer = new URITemplateBasedReferenceProducer(null, WebsocketDownloadEndpoint.DEFAULT_ENDPOINT_URL, WebsocketDownloadEndpoint.DEFAULT_PARAMETER_NAME);
        manager = new TestingResourceManager(catalogBackend, storageBackend,
                referenceProducer,
                new StreamingResourceUploadContexts()
        );
        server = new TestingWebsocketServer().withEndpointConfigs(uploadConfig(), downloadConfig()).start();
        referenceProducer.setBaseUri(server.getURI());
        uploadUri = server.getURI(WebsocketUploadEndpoint.DEFAULT_ENDPOINT_URL);
        logger.info("setUp() done, server=" + server);
    }

    public void tearDown() throws Exception {
        logger.info("tearDown() starting, server=" + server);
        try {
            throw new RuntimeException("This exception is harmless, to diagnose build server behavior");
        } catch (Exception e) {
            logger.info("tearDown() dummy exception", e);
        }
        Thread.sleep(100);
        sessionsHandler.shutdown();
        server.stop();
        storageBackend.cleanup();
        logger.info("tearDown() done");
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
    public synchronized void testLowLevelUploadFollowedByOneShotDownload() throws Exception {
        setUp();
        long DATA_SIZE = 500_000L;
        StreamingResourceMetadata metadata = new StreamingResourceMetadata("test.txt", TEXT_PLAIN, true);
        WebsocketUploadSession upload = new WebsocketUploadSession(metadata, new EndOfInputSignal());
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

        assertEquals(DATA_SIZE, upload.getCurrentStatus().getCurrentSize());
        assertEquals(StreamingResourceTransferStatus.COMPLETED, upload.getCurrentStatus().getTransferStatus());

        assertEquals(DATA_SIZE, downloadClient.requestChunkTransfer(0, DATA_SIZE, OutputStream.nullOutputStream()).get().longValue());
        downloadClient.close();
        tearDown();
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
            input = new LimitedBufferInputStream(new TricklingDelegatingInputStream(new FileInputStream(sourceFile), sourceFile.length(), duration, durationUnit), 64);
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
    public synchronized void testHighLevelUploadWithSimultaneousDownloadsRandomData() throws Exception {
        setUp();
        long DATA_SIZE = 200_000_000L;
        RandomBytesProducer randomBytesProducer = new RandomBytesProducer(DATA_SIZE, 5, TimeUnit.SECONDS);

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
        assertEquals(DATA_SIZE, randomBytesProducer.produce());
        assertEquals(DATA_SIZE, randomBytesProducer.file.length());
        upload.signalEndOfInput();
        logger.info("UPLOAD FINAL STATUS: {}", upload.getFinalStatusFuture().get());
        upload.close(); // optional
        downloadThread.join();
        assertEquals(randomBytesProducer.checksum, downloadChecksum.get());
        // do another transfer, this time of the finished file
        MD5CalculatingOutputStream md5Out = new MD5CalculatingOutputStream(OutputStream.nullOutputStream());
        long again = download.getInputStream().transferTo(md5Out);
        assertEquals(DATA_SIZE, again);
        md5Out.close();
        assertEquals(randomBytesProducer.checksum, md5Out.getChecksum());
        download.close();
        tearDown();
    }

    @Test
    public synchronized void testHighLevelUploadWithSimultaneousDownloadsWithTextConversion() throws Exception {
        setUp();
        URL url = Thread.currentThread().getContextClassLoader().getResource("Faust-8859-1.txt");
        File sourceFile = new File(url.toURI());
        long INPUT_DATA_SIZE = 10171; // in ISO-8859-1 format
        long OUTPUT_DATA_SIZE = 10324; // in UTF-8 format, i.e. what is expected to arrive server-side
        Long OUTPUT_LINES = 296L;
        FileBytesProducer isoBytesProducer = new FileBytesProducer(sourceFile, 5, TimeUnit.SECONDS);

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
        assertEquals(INPUT_DATA_SIZE, isoBytesProducer.produce());
        assertEquals(INPUT_DATA_SIZE, isoBytesProducer.file.length());
        upload.signalEndOfInput();
        StreamingResourceStatus finalUploadStatus = upload.getFinalStatusFuture().join();
        logger.info("UPLOAD FINAL STATUS: {}", finalUploadStatus);
        assertEquals(OUTPUT_LINES, finalUploadStatus.getNumberOfLines());
        assertEquals(OUTPUT_DATA_SIZE, finalUploadStatus.getCurrentSize());
        assertEquals(StreamingResourceTransferStatus.COMPLETED, finalUploadStatus.getTransferStatus());
        upload.close(); // optional
        downloadThread.join();
        assertEquals(FAUST_ISO8859_CHECKSUM, isoBytesProducer.checksum);
        assertEquals(FAUST_UTF8_CHECKSUM, downloadChecksum.get());

        // do another transfer, this time of the finished file
        MD5CalculatingOutputStream md5Out = new MD5CalculatingOutputStream(OutputStream.nullOutputStream());
        long again = download.getInputStream().transferTo(md5Out);
        assertEquals(OUTPUT_DATA_SIZE, again);
        md5Out.close();
        assertEquals(FAUST_UTF8_CHECKSUM, md5Out.getChecksum());
        download.close();
        tearDown();
    }

    @Test
    public synchronized void testErrorScenarios() throws Exception {
        setUp();
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
        IOException io = assertThrows(IOException.class, upload::close);
        assertTrue(io.getMessage().contains("Upload session was closed before input was signalled to be complete"));
        Exception e = assertThrows(Exception.class, () -> upload.getFinalStatusFuture().join());
        assertTrue(e.getMessage().contains("Upload session was closed before input was signalled to be complete"));
        tearDown();
    }

    @Test
    public synchronized void testLineBasedDownload() throws Exception {
        setUp();
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

        // we retrieved the data using line-based access, but this should be exactly equivalent to the raw file
        assertEquals(FAUST_UTF8_CHECKSUM, md5Out.getChecksum());
        tearDown();
    }

    @Test
    public synchronized void testFailedDownload() throws Exception {
        setUp();
        WebsocketUploadProvider uploadProvider = new WebsocketUploadProvider(uploadUri);

        testFailedDownloadWithInput(uploadProvider, "Failing");
        testFailedDownloadWithInput(uploadProvider, "Failing\n");
        tearDown();
    }

    private static void testFailedDownloadWithInput(WebsocketUploadProvider uploadProvider, String input) throws IOException, QuotaExceededException, InterruptedException {
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
            assertEquals(new StreamingResourceStatus(StreamingResourceTransferStatus.FAILED, input.length(), 1L), download.getCurrentStatus());
            String downloaded = data.toString();
            assertEquals(input, downloaded);
        } finally {
            Files.deleteIfExists(dataFile.toPath());
        }
    }


    @Test
    public synchronized void testUploadErrorContextRequired() throws Exception {
        setUp();
        File dataFile = Files.createTempFile("step-streaming-test-", ".txt").toFile();
        WebsocketUploadProvider uploadProvider = new WebsocketUploadProvider(uploadUri);
        // ask manager to require context (but we don't provide one)
        manager.uploadContextRequired = true;
        try {
            IOException e = assertThrows(IOException.class, () -> uploadProvider.startLiveTextFileUpload(dataFile, new StreamingResourceMetadata("dummy.txt", TEXT_PLAIN, true), StandardCharsets.UTF_8));
            assertEquals("WebSocket closed by server: CloseReason[1008,Missing parameter streamingUploadContextId]", e.getMessage());
        } finally {
            Files.deleteIfExists(dataFile.toPath());
        }
        tearDown();
    }

    @Test
    public synchronized void testUploadErrorQuotaExceeded() throws Exception {
        setUp();
        File dataFile = Files.createTempFile("step-streaming-test-", ".txt").toFile();
        WebsocketUploadProvider uploadProvider = new WebsocketUploadProvider(uploadUri);
        manager.quotaExceededException = new QuotaExceededException("oops!");
        try {
            QuotaExceededException e = assertThrows(QuotaExceededException.class, () -> uploadProvider.startLiveTextFileUpload(dataFile, new StreamingResourceMetadata("dummy.txt", TEXT_PLAIN, true), StandardCharsets.UTF_8));
            assertEquals("oops!", e.getMessage());
        } finally {
            Files.deleteIfExists(dataFile.toPath());
        }
        tearDown();
    }

    @Test
    public synchronized void testSizeRestrictionCallback() throws Exception {
        setUp();
        File dataFile = Files.createTempFile("step-streaming-test-", ".txt").toFile();
        StreamingUploads uploads = new StreamingUploads(new WebsocketUploadProvider(uploadUri));
        try {
            StreamingUpload upload = uploads.startTextFileUpload(dataFile);
            manager.sizeChecker = value -> {
                if (value > 10) {
                    throw new QuotaExceededException("Size " + value + " exceeds threshold 10");
                }
            };
            Files.writeString(dataFile.toPath(), "This is the file content.");
            QuotaExceededException e = assertThrows(QuotaExceededException.class, upload::complete);
            assertEquals("Size 25 exceeds threshold 10", e.getMessage());
        } finally {
            Files.deleteIfExists(dataFile.toPath());
        }
        tearDown();
    }
}
