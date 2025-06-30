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
import step.streaming.server.DefaultStreamingResourceReferenceMapper;
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
import step.streaming.websocket.test.TricklingBytesInputStream;

import java.io.*;
import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class StreamingResourceEndpointTests {
    private static final Logger logger = LoggerFactory.getLogger("TEST");
    private TestingStorageBackend storageBackend;
    private InMemoryCatalogBackend catalogBackend;
    private DefaultStreamingResourceManager manager;
    private WebsocketServerEndpointSessionsHandler sessionsHandler;

    @Before
    public void setUp() throws IOException {
        sessionsHandler = DefaultWebsocketServerEndpointSessionsHandler.getInstance();
        storageBackend = new TestingStorageBackend(1000L, false);
        catalogBackend = new InMemoryCatalogBackend();
        manager = new DefaultStreamingResourceManager(catalogBackend, storageBackend,
                new DefaultStreamingResourceReferenceMapper(null,
                        WebsocketDownloadEndpoint.DEFAULT_ENDPOINT_URL,
                        WebsocketDownloadEndpoint.DEFAULT_PARAMETER_PLACEHOLDER)
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
                        return endpointClass.cast(new WebsocketDownloadEndpoint(manager, sessionsHandler));
                    }
                })
                .build();
    }

    @Test
    public void testLowLevelUploadFollowedByOneShotDownload() throws Exception {
        long DATA_SIZE = 500_000L;
        TestingWebsocketServer server = new TestingWebsocketServer().withEndpointConfigs(uploadConfig(), downloadConfig()).start();
        ((DefaultStreamingResourceReferenceMapper) manager.getReferenceMapper()).setBaseUri(server.getURI());
        URI uploadUri = server.getURI(WebsocketUploadEndpoint.DEFAULT_ENDPOINT_URL);

        StreamingResourceMetadata metadata = new StreamingResourceMetadata("test.txt", StreamingResourceMetadata.CommonMimeTypes.TEXT_PLAIN);
        WebsocketUpload upload = new WebsocketUpload(metadata, null);
        // TODO: timeout
        //WebsocketUploadClient uploadClient = new WebsocketUploadClient(uploadUri, upload, ContainerProvider.getWebSocketContainer(), 200, 10);
        WebsocketUploadClient uploadClient = new WebsocketUploadClient(uploadUri, upload);
        InputStream data = new TricklingBytesInputStream(DATA_SIZE, 5, TimeUnit.SECONDS);

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

    private static class DataProducer {
        File file;
        final InputStream input;
        final MD5CalculatingOutputStream output;
        String checksum;

        public DataProducer(long bytes, long duration, TimeUnit durationUnit) throws Exception {
            file = File.createTempFile("streaming-upload-test", ".tmp");
            file.deleteOnExit();
            input = new TricklingBytesInputStream(bytes, duration, durationUnit);
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
    public void testHighLevelUploadWithSimultaneousDownloads() throws Exception {
        long DATA_SIZE = 200_000_000L;
        DataProducer dataProducer = new DataProducer(DATA_SIZE, 5, TimeUnit.SECONDS);

        TestingWebsocketServer server = new TestingWebsocketServer().withEndpointConfigs(uploadConfig(), downloadConfig()).start();
        URI uploadUri = server.getURI(WebsocketUploadEndpoint.DEFAULT_ENDPOINT_URL);
        ((DefaultStreamingResourceReferenceMapper) manager.getReferenceMapper()).setBaseUri(server.getURI());

        WebsocketUploadProvider provider = new WebsocketUploadProvider(uploadUri);
        StreamingUpload upload = provider.startLiveFileUpload(dataProducer.file, new StreamingResourceMetadata("test.txt", StreamingResourceMetadata.CommonMimeTypes.TEXT_PLAIN));
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
        Assert.assertEquals(DATA_SIZE, dataProducer.produce());
        Assert.assertEquals(DATA_SIZE, dataProducer.file.length());
        logger.info("UPLOAD FINAL STATUS: {}", upload.signalEndOfInput().get());
        upload.close(); // optional
        downloadThread.join();
        Assert.assertEquals(dataProducer.checksum, downloadChecksum.get());
        // do another transfer, this time of the finished file
        MD5CalculatingOutputStream md5Out = new MD5CalculatingOutputStream(OutputStream.nullOutputStream());
        long again = download.getInputStream().transferTo(md5Out);
        Assert.assertEquals(DATA_SIZE, again);
        md5Out.close();
        Assert.assertEquals(dataProducer.checksum, md5Out.getChecksum());
        download.close();
        Thread.sleep(50);
        sessionsHandler.shutdown();
        server.stop();
    }
}
