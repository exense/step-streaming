package step.streaming.client.upload;

import step.streaming.client.StreamingTransfer;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.data.EndOfInputSignal;
import step.streaming.util.ThrowingConsumer;

import java.util.concurrent.*;

/**
 * Lower-level interface for managing the lifecycle of a streaming upload.
 * <p>
 * An instance of this interface represents a single active upload session and
 * provides direct control over signaling completion, cancellation, and
 * asynchronous status retrieval.
 * <p>
 * Implementations are created by a {@link StreamingUploadProvider}.
 */
public interface StreamingUploadSession extends StreamingTransfer {

    /**
     * Returns a {@link CompletableFuture} that will be completed with the final
     * {@link StreamingResourceStatus} of the upload.
     * <p>
     * The returned future completes:
     * <ul>
     *   <li>successfully, with the final status, when the upload finishes normally, or</li>
     *   <li>exceptionally, if the upload fails or is cancelled.</li>
     * </ul>
     *
     * @return a future indicating the final status of the upload.
     */
    CompletableFuture<StreamingResourceStatus> getFinalStatusFuture();

    /**
     * Returns the {@link StreamingResourceMetadata} of the upload being performed.
     *
     * @return the upload metadata.
     */
    StreamingResourceMetadata getMetadata();

    /**
     * Returns the {@link EndOfInputSignal} associated with this upload.
     * <p>
     * This signal can be used for advanced control over the input side of the upload.
     * It is typically completed normally to indicate that no more input data will
     * be provided, or completed exceptionally to indicate cancellation or failure.
     * <p>
     * Advanced users may interact with the underlying {@link CompletableFuture} methods
     * on the signal to control completion behavior directly.
     *
     * @return the end-of-input signal.
     */
    EndOfInputSignal getEndOfInputSignal();


    /**
     * Signals that all input for this upload has been provided and no more data
     * will be sent.
     * <p>
     * This is done by completing the end-of-input signal normally. Once this method
     * is invoked, the upload will proceed to completion unless it fails.
     */
    default void signalEndOfInput() {
        getEndOfInputSignal().complete(null);
    }

    /**
     * Cancels the upload by completing the end-of-input signal exceptionally.
     * <p>
     * If {@code optionalCause} is non-{@code null}, it will be used as the
     * exception that completes the signal. Otherwise, a default
     * {@link CancellationException} with the message {@code "Cancelled by user"}
     * will be used.
     *
     * @param optionalCause the reason for cancellation, or {@code null} to use
     *                      a default {@link CancellationException}.
     */
    default void cancel(Throwable optionalCause) {
        getEndOfInputSignal().completeExceptionally(optionalCause != null ? optionalCause : new CancellationException("Cancelled by user"));
    }

    void onClose(ThrowingConsumer<String> onCloseCallback);
}
