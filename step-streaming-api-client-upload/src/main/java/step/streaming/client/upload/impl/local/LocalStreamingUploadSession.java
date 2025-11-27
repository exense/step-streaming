package step.streaming.client.upload.impl.local;

import step.streaming.client.upload.impl.BasicStreamingUploadSession;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.data.CheckpointingOutputStream;
import step.streaming.data.EndOfInputSignal;
import step.streaming.data.LinebreakDetectingOutputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

public class LocalStreamingUploadSession extends BasicStreamingUploadSession {

    private final InputStream inputStream;
    private final OutputStream targetOutputStream;

    public LocalStreamingUploadSession(InputStream inputStream, OutputStream outputStream, StreamingResourceMetadata metadata, EndOfInputSignal endOfInputSignal) {
        super(metadata, endOfInputSignal);
        this.inputStream = Objects.requireNonNull(inputStream);
        this.targetOutputStream = Objects.requireNonNull(outputStream);
    }

    void transfer() {
        OutputStream actualOutputStream = null;
        try {
            actualOutputStream = new CheckpointingOutputStream(this.targetOutputStream, CheckpointingOutputStream.DEFAULT_FLUSH_INTERVAL_MILLIS, this::updateInProgressTransferSize);
            // this is easier to use in lambdas than AtomicLong and does the job just as well
            Long[] counter = new Long[1];
            if (metadata.getSupportsLineAccess()) {
                counter[0] = 0L;
                actualOutputStream = new LinebreakDetectingOutputStream(actualOutputStream, position -> counter[0]++);
            }
            long transferred = inputStream.transferTo(actualOutputStream);
            finalStatusFuture.complete(new StreamingResourceStatus(StreamingResourceTransferStatus.COMPLETED, transferred, counter[0]));
        } catch (IOException e) {
            finalStatusFuture.completeExceptionally(e);
        } finally {
            try {
                inputStream.close();
            } catch (IOException ignored) {
            }
            if (actualOutputStream != null) {
                try {
                    actualOutputStream.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    // This behaves exactly like the Websocket implementation:
    // line numbers are normally counted server-side and only available after the upload ended
    private void updateInProgressTransferSize(long transferredBytes) {
        if (!getFinalStatusFuture().isDone()) {
            setCurrentStatus(new StreamingResourceStatus(StreamingResourceTransferStatus.IN_PROGRESS, transferredBytes, null));
        }
    }
}
