package step.streaming.websocket.server.upload;

import jakarta.websocket.ContainerProvider;
import jakarta.websocket.WebSocketContainer;
import jakarta.websocket.server.ServerEndpointConfig;
import org.eclipse.jetty.util.component.LifeCycle;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;
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
import step.streaming.websocket.test.ThreadPools;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.*;

public class StreamingResourceEndpointTests {

    private static final String TEXT_PLAIN = "text/plain";
    private static final String APPLICATION_OCTET_STREAM = "application/octet-stream";


    // aaaaaaargh... surefire consistently messes up these tests because it parallelizes them!
    private static final Lock CLASS_LOCK = new ReentrantLock();
    @Rule
    public final TestRule serializeMethods = (base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            CLASS_LOCK.lock();
            try {
                base.evaluate(); // run the test method
            } finally {
                CLASS_LOCK.unlock();
            }
        }
    };

    private static final Logger logger = LoggerFactory.getLogger("UNITTEST");
    private WebSocketContainer wsContainer;
    private TestingStorageBackend storageBackend;
    private InMemoryCatalogBackend catalogBackend;
    private TestingResourceManager manager;
    private WebsocketServerEndpointSessionsHandler sessionsHandler;
    private URITemplateBasedReferenceProducer referenceProducer;
    private TestingWebsocketServer server;
    private URI uploadUri;
    private ExecutorService clientsExecutor;
    private ExecutorService serverExecutor;

    private static final String FAUST_ISO8859_CHECKSUM = "317c7a8df8c817c80bf079cfcbbc6686";
    private static final String FAUST_UTF8_CHECKSUM = "540441d13a31641d7775d91c46c94511";

    @Before
    public void setUp() throws Exception {
        clientsExecutor = ThreadPools.createPoolExecutor("clients-executor");
        serverExecutor = ThreadPools.createPoolExecutor("websocket-upload-processor");
        wsContainer = ContainerProvider.getWebSocketContainer();
        sessionsHandler = new DefaultWebsocketServerEndpointSessionsHandler();
        storageBackend = new TestingStorageBackend(1000L, false);
        catalogBackend = new InMemoryCatalogBackend();
        referenceProducer = new URITemplateBasedReferenceProducer(null, WebsocketDownloadEndpoint.DEFAULT_ENDPOINT_URL, WebsocketDownloadEndpoint.DEFAULT_PARAMETER_NAME);
        manager = new TestingResourceManager(catalogBackend, storageBackend,
                referenceProducer,
                new StreamingResourceUploadContexts(),
                serverExecutor
        );
        server = new TestingWebsocketServer().withEndpointConfigs(uploadConfig(), downloadConfig()).start();
        referenceProducer.setBaseUri(server.getURI());
        uploadUri = server.getURI(WebsocketUploadEndpoint.DEFAULT_ENDPOINT_URL);
    }

    @After
    public void tearDown() throws Exception {
        Thread.sleep(100);
        sessionsHandler.shutdown();
        clientsExecutor.shutdownNow();
        serverExecutor.shutdownNow();
        ((LifeCycle) wsContainer).stop();
        server.stop();
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
    @Ignore
    // for repeatedly running a particular test
    public void adNauseam() throws Exception {
        for (int i = 0; i < 100; ++i) {
            testHighLevelUploadWithSimultaneousDownloadsWithTextConversion();
            Thread.sleep(3000);
        }
    }

    @Test
    public void testLowLevelUploadFollowedByOneShotDownload() throws Exception {
        long DATA_SIZE = 500_000L;
        StreamingResourceMetadata metadata = new StreamingResourceMetadata("test.txt", TEXT_PLAIN, true);
        WebsocketUploadSession upload = new WebsocketUploadSession(metadata, new EndOfInputSignal());
        // TODO: timeout
        //WebsocketUploadClient uploadClient = new WebsocketUploadClient(uploadUri, upload, ContainerProvider.getWebSocketContainer(), 200, 10);
        WebsocketUploadClient uploadClient = new WebsocketUploadClient(wsContainer, uploadUri, upload);
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
    public void testHighLevelUploadWithSimultaneousDownloadsRandomData() throws Exception {
        long DATA_SIZE = 200_000_000L;
        RandomBytesProducer randomBytesProducer = new RandomBytesProducer(DATA_SIZE, 5, TimeUnit.SECONDS);

        WebsocketUploadProvider provider = new WebsocketUploadProvider(wsContainer, clientsExecutor, uploadUri);
        StreamingUploadSession upload = provider.startLiveBinaryFileUpload(randomBytesProducer.file, new StreamingResourceMetadata("test.bin", APPLICATION_OCTET_STREAM, false));
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
    }

    @Test
    public void testHighLevelUploadWithSimultaneousDownloadsWithTextConversion() throws Exception {
        URL url = Thread.currentThread().getContextClassLoader().getResource("Faust-8859-1.txt");
        File sourceFile = new File(url.toURI());
        long INPUT_DATA_SIZE = 10171; // in ISO-8859-1 format
        long OUTPUT_DATA_SIZE = 10324; // in UTF-8 format, i.e. what is expected to arrive server-side
        Long OUTPUT_LINES = 296L;
        FileBytesProducer isoBytesProducer = new FileBytesProducer(sourceFile, 5, TimeUnit.SECONDS);

        WebsocketUploadProvider provider = new WebsocketUploadProvider(wsContainer, clientsExecutor, uploadUri);
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
    }

    @Test
    public void testErrorScenarios() throws Exception {
        File sourceFile = new File(Thread.currentThread().getContextClassLoader().getResource("Faust-8859-1.txt").toURI());
        FileBytesProducer uploadProducer = new FileBytesProducer(sourceFile, 15, TimeUnit.SECONDS);

        WebsocketUploadProvider provider = new WebsocketUploadProvider(wsContainer, clientsExecutor, uploadUri);
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
    }

    @Test
    public void testLineBasedDownload() throws Exception {
        File sourceFile = new File(Thread.currentThread().getContextClassLoader().getResource("Faust-8859-1.txt").toURI());
        FileBytesProducer uploadProducer = new FileBytesProducer(sourceFile, 15, TimeUnit.SECONDS);

        WebsocketUploadProvider provider = new WebsocketUploadProvider(wsContainer, clientsExecutor, uploadUri);
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
                logger.info("status received: {}, linesReceived={} => linesToFetch={}", status.get(), linesReceived.get(), linesToFetch);
                if (status.get().getTransferStatus().equals(StreamingResourceTransferStatus.FAILED)) {
                    completed.completeExceptionally(new IllegalStateException("" + StreamingResourceTransferStatus.FAILED));
                }
                if (linesToFetch > 0) {
                    try {
                        logger.info("Calling requestTextLines({},{},...)", linesReceived.get(), linesToFetch);
                        downloadClient.requestTextLines(linesReceived.get(), linesToFetch, lines -> {
                            linesReceived.addAndGet(lines.size());
                            logger.info("Received {} lines -> {}", lines.size(), linesReceived.get());
                            for (String line : lines) {
                                try {
                                    md5Out.write(line.getBytes(StandardCharsets.UTF_8));
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                            var ns = status.get();
                            if (ns.getTransferStatus().equals(StreamingResourceTransferStatus.COMPLETED) && ns.getNumberOfLines() == linesReceived.get()) {
                                logger.info("Status is completed(2), finishing");
                                completed.complete(true);
                            }
                        });
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } else if (status.get().getTransferStatus().equals(StreamingResourceTransferStatus.COMPLETED)) {
                    logger.info("Status is completed(1), finishing");
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
    }

    @Test
    public void testFailedDownload() throws Exception {
        WebsocketUploadProvider uploadProvider = new WebsocketUploadProvider(wsContainer, clientsExecutor, uploadUri);

        testFailedDownloadWithInput(uploadProvider, "Failing");
        testFailedDownloadWithInput(uploadProvider, "Failing\n");
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
    public void testUploadErrorContextRequired() throws Exception {
        File dataFile = Files.createTempFile("step-streaming-test-", ".txt").toFile();
        WebsocketUploadProvider uploadProvider = new WebsocketUploadProvider(wsContainer, clientsExecutor, uploadUri);
        // ask manager to require context (but we don't provide one)
        manager.uploadContextRequired = true;
        try {
            Exception e = assertThrows(Exception.class, () -> uploadProvider.startLiveTextFileUpload(dataFile, new StreamingResourceMetadata("dummy.txt", TEXT_PLAIN, true), StandardCharsets.UTF_8));
            assertTrue(e.getMessage().contains("CloseReason[1008,Missing parameter streamingUploadContextId]"));
        } finally {
            Files.deleteIfExists(dataFile.toPath());
        }
    }

    @Test
    public void testUploadErrorQuotaExceeded() throws Exception {
        File dataFile = Files.createTempFile("step-streaming-test-", ".txt").toFile();
        WebsocketUploadProvider uploadProvider = new WebsocketUploadProvider(wsContainer, clientsExecutor, uploadUri);
        manager.quotaExceededException = new QuotaExceededException("oops!");
        try {
            QuotaExceededException e = assertThrows(QuotaExceededException.class, () -> uploadProvider.startLiveTextFileUpload(dataFile, new StreamingResourceMetadata("dummy.txt", TEXT_PLAIN, true), StandardCharsets.UTF_8));
            assertEquals("oops!", e.getMessage());
        } finally {
            Files.deleteIfExists(dataFile.toPath());
        }
    }

    @Test
    public void testSizeRestrictionCallback() throws Exception {
        File dataFile = Files.createTempFile("step-streaming-test-", ".txt").toFile();
        StreamingUploads uploads = new StreamingUploads(new WebsocketUploadProvider(wsContainer, clientsExecutor, uploadUri));
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
    }

    @Test
    public void testZeroByteUpload() throws Exception {
        File dataFile = Files.createTempFile("step-streaming-test-", ".txt").toFile();
        StreamingUploads uploads = new StreamingUploads(new WebsocketUploadProvider(wsContainer, clientsExecutor, uploadUri));
        try {
            StreamingUpload upload = uploads.startTextFileUpload(dataFile);
            Thread.sleep(500);
            var result = upload.complete();
            Assert.assertEquals(new StreamingResourceStatus(StreamingResourceTransferStatus.COMPLETED, 0, 0L), result);
        } finally {
            Files.deleteIfExists(dataFile.toPath());
        }
    }
}
