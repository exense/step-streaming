package step.streaming.client.upload.impl.local;

import step.streaming.client.upload.impl.AbstractStreamingUploadProvider;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.data.EndOfInputSignal;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;

/**
 * A {@link step.streaming.client.upload.StreamingUploadProvider} that behaves like a real upload provider,
 * but discards the data instead of storing it.
 */
public class DiscardingStreamingUploadProvider extends AbstractStreamingUploadProvider {

    public DiscardingStreamingUploadProvider(ExecutorService executorService) {
        super(executorService);
    }

    @Override
    protected LocalStreamingUploadSession startLiveFileUpload(InputStream sourceInputStream, StreamingResourceMetadata metadata, EndOfInputSignal endOfInputSignal) {
        LocalStreamingUploadSession session = new LocalStreamingUploadSession(sourceInputStream, OutputStream.nullOutputStream(), metadata, endOfInputSignal);
        executorService.submit(session::transfer);
        return session;
    }
}
