package step.streaming.websocket.client.upload;

import step.streaming.client.AbstractTransfer;
import step.streaming.data.EndOfInputSignal;
import step.streaming.client.upload.StreamingUploadSession;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceReference;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.data.util.ThrowingConsumer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Represents a streaming upload using a WebSocket transport.
 * <p>
 */
public class WebsocketUploadSession extends AbstractTransfer implements StreamingUploadSession {
    private final EndOfInputSignal endOfInputSignal;
    private final List<ThrowingConsumer<String>> onCloseCallbacks = new CopyOnWriteArrayList<>();
    private final CompletableFuture<StreamingResourceStatus> finalStatusFuture = new CompletableFuture<>();
    private final StreamingResourceMetadata metadata;

    /**
     * Creates a new {@code WebsocketUpload} instance for a streaming resource upload.
     *
     * @param metadata the metadata associated with the resource being uploaded (e.g., filename, MIME type)
     * @param endOfInputSignal a signal used to indicate when all input data has been written and the upload is complete;
     *                         may be {@code null} for uploads that do not require explicit signaling
     */
    public WebsocketUploadSession(StreamingResourceMetadata metadata, EndOfInputSignal endOfInputSignal) {
        this.metadata = metadata;
        this.endOfInputSignal = endOfInputSignal;
        // This will perform a final update of the current status on completion.
        finalStatusFuture.whenComplete((status, throwable) -> {
            if (throwable == null) {
                setCurrentStatus(status);
            } else {
                Optional<StreamingResourceStatus> lastStatus = Optional.ofNullable(getCurrentStatus());
                setCurrentStatus(new StreamingResourceStatus(StreamingResourceTransferStatus.FAILED,
                        lastStatus.map(StreamingResourceStatus::getCurrentSize).orElse(0L),
                        lastStatus.map(StreamingResourceStatus::getNumberOfLines).orElse(null)));
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
    public void close() throws IOException {
        String closeMessage = "Upload session closed";
        if (hasEndOfInputSignal() && !endOfInputSignal.isDone()) {
            closeMessage = "Upload session was closed before input was signalled to be complete";
            endOfInputSignal.completeExceptionally(new CancellationException(closeMessage));
        }
        AtomicReference<IOException> resultException = new AtomicReference<>(null);
        Consumer<Exception> onException = e -> {
            if (resultException.get() == null) {
                IOException io = e instanceof IOException ? (IOException) e : new IOException(e);
                resultException.set(io);
            } else {
                resultException.get().addSuppressed(e);
            }
        };
        for (ThrowingConsumer<String> onCloseCallback : onCloseCallbacks) {
            try {
                onCloseCallback.accept(closeMessage);
            } catch (Exception e) {
                onException.accept(e);
            }
        }
        // Wait for the final status to be set. If not set before,
        // it will be set when closing the upload client (potentially
        // throwing an exception if the upload has not completed normally)
        try {
            finalStatusFuture.get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            onException.accept(e);
        }
        if (resultException.get() != null) {
            throw resultException.get();
        }
    }

    void onClose(ThrowingConsumer<String> onCloseCallback) {
        onCloseCallbacks.add(onCloseCallback);
    }

}
