package step.streaming.websocket.client.upload;

import step.streaming.client.AbstractTransfer;
import step.streaming.data.EndOfInputSignal;
import step.streaming.client.upload.StreamingUpload;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceReference;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Represents a streaming upload using a WebSocket transport.
 * <p>
 */
public class WebsocketUpload extends AbstractTransfer implements StreamingUpload {
    private final EndOfInputSignal endOfInputSignal;
    private final List<Runnable> onCloseCallbacks = new CopyOnWriteArrayList<>();
    private final CompletableFuture<StreamingResourceStatus> finalStatusFuture = new CompletableFuture<>();
    private final StreamingResourceMetadata metadata;

    /**
     * Creates a new {@code WebsocketUpload} instance for a streaming resource upload.
     *
     * @param metadata the metadata associated with the resource being uploaded (e.g., filename, MIME type)
     * @param endOfInputSignal a signal used to indicate when all input data has been written and the upload is complete;
     *                         may be {@code null} for uploads that do not require explicit signaling
     */
    public WebsocketUpload(StreamingResourceMetadata metadata, EndOfInputSignal endOfInputSignal) {
        this.metadata = metadata;
        this.endOfInputSignal = endOfInputSignal;
        // This will perform a final update of the current status on completion.
        finalStatusFuture.whenComplete((status, throwable) -> {
            if (throwable == null) {
                setCurrentStatus(status);
            } else {
                // FIXME: what's the size?
                setCurrentStatus(new StreamingResourceStatus(StreamingResourceTransferStatus.FAILED, 0, null));
            }
        });
    }

    public StreamingResourceMetadata getMetadata() {
        return metadata;
    }

    // the following two methods are protected by the superclass, so we need to expose them under a different name
    void setUploadReference(StreamingResourceReference reference) {
        setReference(reference);
    }

    void setCurrentUploadStatus(StreamingResourceStatus status) {
        super.setCurrentStatus(status);
    }

    @Override
    public boolean hasEndOfInputSignal() {
        return endOfInputSignal != null;
    }

    @Override
    public EndOfInputSignal getEndOfInputSignal() {
        return endOfInputSignal;
    }

    @Override
    public CompletableFuture<StreamingResourceStatus> getFinalStatusFuture() {
        return finalStatusFuture;
    }


    @Override
    public void close() {
        if (hasEndOfInputSignal() && !endOfInputSignal.isDone()) {
            endOfInputSignal.completeExceptionally(new CancellationException("Upload closed before input was signalled to be complete"));
        }
        onCloseCallbacks.forEach(Runnable::run);
    }

    void onClose(Runnable onCloseCallback) {
        onCloseCallbacks.add(onCloseCallback);
    }

}
