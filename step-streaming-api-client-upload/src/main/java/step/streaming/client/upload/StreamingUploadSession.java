package step.streaming.client.upload;

import step.streaming.client.StreamingTransfer;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.data.EndOfInputSignal;

import java.util.concurrent.*;

/**
 * High-level interface for handling uploads.
 * This is returned by the {@link StreamingUploadProvider}.
 * <p>
 * It is generally expected that uploads use an end-of-input signal.
 * Providers might also expose methods that allow to create uploads
 * from standard Java InputStreams (in which case end-of-input would
 * be signaled by the stream itself).
 * <p>
 * Note: If no end-of-input signal is used, the cancellation methods
 * will not be able to reliably interrupt the upload, and the signaling
 * methods will effectively be a no-op.
 *
 * @see StreamingUploadSession#getEndOfInputSignal()
 */
public interface StreamingUploadSession extends StreamingTransfer {
    /**
     * Returns a future indicating the final status of the upload.
     * This future may be completed successfully, or exceptionally.
     * @return a future indicating the final status of the upload.
     */
    CompletableFuture<StreamingResourceStatus> getFinalStatusFuture();

    default StreamingResourceStatus getFinalStatus() {
        return getFinalStatusFuture().join();
    }

    /**
     * Indicates whether this upload uses an end-of-input signal.
     * @return <tt>true</tt> if an end-of-input signal is present, <tt>false</tt> otherwise
     */
    boolean hasEndOfInputSignal();

    /**
     * Returns the end-of-input signal, if present, <tt>null</tt> otherwise.
     * This can be used for direct control, i.e. advanced use of the methods
     * exposed by the underlying {@link CompletableFuture}. It is generally
     * advised to use the {@link #signalEndOfInput()}, {@link #cancel()},
     * or {@link #cancel(Exception)} though.
     * @return the end-of-input signal, or null.
     */
    EndOfInputSignal getEndOfInputSignal();

    /**
     * Explicitly signals that the input file is complete.
     */
    default void signalEndOfInput() {
        if (hasEndOfInputSignal()) getEndOfInputSignal().complete(null);
    }

    /** Cancels the upload by signaling an exception on the end-of-input signal.
     */
    default void cancel() {
        cancel(new CancellationException("Cancelled by user"));
    }

    /** Cancels the upload by signaling a user-supplied exception on the end-of-input signal.
     *
     * @param cause cancellation cause
     */
    default void cancel(Exception cause) {
        if (hasEndOfInputSignal()) {
            getEndOfInputSignal().completeExceptionally(cause);
        }
    }
}
