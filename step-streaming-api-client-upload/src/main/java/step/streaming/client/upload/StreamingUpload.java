package step.streaming.client.upload;

import step.streaming.client.StreamingTransfer;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.data.EndOfInputSignal;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

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
 * @see StreamingUpload#getEndOfInputSignal()
 */
public interface StreamingUpload extends StreamingTransfer {
    /**
     * Returns a future indicating the final status of the upload.
     * This future may be completed successfully, or exceptionally.
     * @return a future indicating the final status of the upload.
     */
    CompletableFuture<StreamingResourceStatus> getFinalStatusFuture();

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
     * Convenience method to signal end of input, and retrieve the final status future at the same time.
     * @return the final status future
     */
    default CompletableFuture<StreamingResourceStatus> signalEndOfInput() {
        if (hasEndOfInputSignal()) getEndOfInputSignal().complete(null);
        return getFinalStatusFuture();
    }

    /** Cancels the upload by signaling an exception on the end-of-input signal.
     *
     * @return the final status future
     */
    default CompletableFuture<StreamingResourceStatus> cancel() {
        return cancel(new CancellationException("Cancelled by user"));
    }

    /** Cancels the upload by signaling a user-supplied exception on the end-of-input signal.
     *
     * @param cause cancellation cause
     * @return the final status future
     */
    default CompletableFuture<StreamingResourceStatus> cancel(Exception cause) {
        if (hasEndOfInputSignal()) getEndOfInputSignal().completeExceptionally(cause);
        return getFinalStatusFuture();
    }
}
