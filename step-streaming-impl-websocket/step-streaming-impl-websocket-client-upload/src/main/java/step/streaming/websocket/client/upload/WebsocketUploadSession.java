package step.streaming.websocket.client.upload;

import step.streaming.client.AbstractTransfer;
import step.streaming.client.upload.StreamingUploadSession;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceReference;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;
import step.streaming.data.EndOfInputSignal;
import step.streaming.data.util.ThrowingConsumer;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

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
        this.endOfInputSignal = Objects.requireNonNull(endOfInputSignal);
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

    @Override
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
    public EndOfInputSignal getEndOfInputSignal() {
        return endOfInputSignal;
    }

    @Override
    public CompletableFuture<StreamingResourceStatus> getFinalStatusFuture() {
        return finalStatusFuture;
    }


    private IOException setOrAddSuppressed(IOException existing, Exception current) {
        if (existing == null) {
            // in theory, all incoming exceptions should already be IOExceptions, but wrap others just in case
            return current instanceof IOException ? (IOException) current : new IOException(current);
        } else {
            existing.addSuppressed(current);
            return existing;
        }
    }

    @Override
    public void close() throws IOException {
        String closeMessage = "Upload session closed";
        if (!endOfInputSignal.isDone()) {
            closeMessage = "Upload session was closed before input was signalled to be complete";
            endOfInputSignal.completeExceptionally(new CancellationException(closeMessage));
        }
        IOException exception = null;
        for (ThrowingConsumer<String> onCloseCallback : onCloseCallbacks) {
            try {
                onCloseCallback.accept(closeMessage);
            } catch (Exception e) {
                exception = setOrAddSuppressed(exception, e);
            }
        }
        // Wait for the final status to be set. If not set before,
        // it will be set when closing the upload client (potentially
        // throwing an exception if the upload has not completed normally)
        try {
            finalStatusFuture.get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
            exception = setOrAddSuppressed(exception, e);
        }
        if (exception != null) {
            throw exception;
        }
    }

    void onClose(ThrowingConsumer<String> onCloseCallback) {
        onCloseCallbacks.add(onCloseCallback);
    }

    @Override
    public String toString() {
        return String.format("WebsocketUploadSession{metadata=%s, currentStatus=%s}", metadata, getCurrentStatus());
    }
}
