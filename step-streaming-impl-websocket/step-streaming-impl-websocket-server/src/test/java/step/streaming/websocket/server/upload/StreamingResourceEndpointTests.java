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
import step.streaming.client.upload.StreamingUpload;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.data.CheckpointingOutputStream;
import step.streaming.data.MD5CalculatingInputStream;
import step.streaming.data.MD5CalculatingOutputStream;
import step.streaming.server.DefaultStreamingResourceManager;
import step.streaming.server.URITemplateBasedReferenceProducer;
import step.streaming.server.test.InMemoryCatalogBackend;
import step.streaming.server.test.TestingStorageBackend;
import step.streaming.websocket.client.upload.WebsocketUpload;
import step.streaming.websocket.client.upload.WebsocketUploadClient;
import step.streaming.websocket.client.upload.WebsocketUploadProvider;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class StreamingResourceEndpointTests {
    private static final Logger logger = LoggerFactory.getLogger("TEST");
    private TestingStorageBackend storageBackend;
    private InMemoryCatalogBackend catalogBackend;
    private DefaultStreamingResourceManager manager;
    private WebsocketServerEndpointSessionsHandler sessionsHandler;
    private URITemplateBasedReferenceProducer referenceProducer;

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

        StreamingResourceMetadata metadata = new StreamingResourceMetadata("test.txt", StreamingResourceMetadata.CommonMimeTypes.TEXT_PLAIN);
        WebsocketUpload upload = new WebsocketUpload(metadata, null);
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

        Assert.assertEquals(DATA_SIZE, upload.getCurrentStatus().getCurrentSize().longValue());
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
            input = new TricklingDelegatingInputStream(new FileInputStream(sourceFile), sourceFile.length(), duration, durationUnit);
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
        StreamingUpload upload = provider.startLiveBinaryFileUpload(randomBytesProducer.file, new StreamingResourceMetadata("test.bin", StreamingResourceMetadata.CommonMimeTypes.APPLICATION_OCTET_STREAM));
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
        logger.info("UPLOAD FINAL STATUS: {}", upload.signalEndOfInput().get());
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
        String INPUT_CHECKSUM = "317c7a8df8c817c80bf079cfcbbc6686";
        long OUTPUT_DATA_SIZE = 10324; // in UTF-8 format, i.e. what is expected to arrive server-side
        String OUTPUT_CHECKSUM = "540441d13a31641d7775d91c46c94511";
        FileBytesProducer isoBytesProducer = new FileBytesProducer(sourceFile, 5, TimeUnit.SECONDS);

        TestingWebsocketServer server = new TestingWebsocketServer().withEndpointConfigs(uploadConfig(), downloadConfig()).start();
        URI uploadUri = server.getURI(WebsocketUploadEndpoint.DEFAULT_ENDPOINT_URL);
        referenceProducer.setBaseUri(server.getURI());

        WebsocketUploadProvider provider = new WebsocketUploadProvider(uploadUri);
        // This will transcode the file to UTF-8 on upload (on the fly)
        StreamingUpload upload = provider.startLiveTextFileUpload(isoBytesProducer.file, new StreamingResourceMetadata("faust.txt", StreamingResourceMetadata.CommonMimeTypes.TEXT_PLAIN), StandardCharsets.ISO_8859_1);
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
        logger.info("UPLOAD FINAL STATUS: {}", upload.signalEndOfInput().get());
        upload.close(); // optional
        downloadThread.join();
        Assert.assertEquals(INPUT_CHECKSUM, isoBytesProducer.checksum);
        Assert.assertEquals(OUTPUT_CHECKSUM, downloadChecksum.get());

        // do another transfer, this time of the finished file
        MD5CalculatingOutputStream md5Out = new MD5CalculatingOutputStream(OutputStream.nullOutputStream());
        long again = download.getInputStream().transferTo(md5Out);
        Assert.assertEquals(OUTPUT_DATA_SIZE, again);
        md5Out.close();
        Assert.assertEquals(OUTPUT_CHECKSUM, md5Out.getChecksum());
        download.close();
        Thread.sleep(50);
        sessionsHandler.shutdown();
        server.stop();
    }

}
