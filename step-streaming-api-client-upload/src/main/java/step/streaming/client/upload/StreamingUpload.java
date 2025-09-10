package step.streaming.client.upload;

import step.streaming.common.QuotaExceededException;
import step.streaming.common.StreamingResourceStatus;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * High-level object for performing streaming uploads.
 * <p>
 * An upload MUST be completed by invoking either {@link #complete()} or
 * {@link #complete(Duration)}, which signals that the input data is complete
 * (no more data will be sent), then waits until the upload transfer finishes
 * and the final {@link StreamingResourceStatus} result becomes available.
 * <p>
 * If lower-level control is required (such as manually interacting with the
 * underlying asynchronous constructs), the underlying {@link StreamingUploadSession}
 * can be accessed via {@link #getSession()}.
 */
public class StreamingUpload {
    private final StreamingUploadSession session;


    /**
     * Creates a new {@code StreamingUpload} instance backed by the specified session.
     *
     * @param session the {@link StreamingUploadSession} that performs and manages the actual upload;
     *                must not be {@code null}.
     * @throws NullPointerException if {@code session} is {@code null}.
     */
    public StreamingUpload(StreamingUploadSession session) {
        this.session = Objects.requireNonNull(session);
    }

    /**
     * Returns the underlying {@link StreamingUploadSession} for advanced usage.
     * <p>
     * This allows direct interaction with the upload session for lower-level
     * control, such as directly managing end-of-input signaling, cancellation,
     * or interacting with asynchronous completion futures.
     *
     * @return the underlying {@link StreamingUploadSession}.
     */
    public StreamingUploadSession getSession() {
        return session;
    }

    /**
     * Signals that the upload input is complete and waits for the final upload transfer status,
     * up to the specified timeout.
     * <p>
     * This method first calls {@link StreamingUploadSession#signalEndOfInput()} to indicate that
     * the input is complete and no more data will be provided, and then blocks until either:
     * <ul>
     *   <li>the final {@link StreamingResourceStatus} becomes available, indicating upload completion, or</li>
     *   <li>the given timeout expires.</li>
     * </ul>
     *
     * @param timeout the maximum time to wait for the final status; must not be {@code null}.
     * @return the final {@link StreamingResourceStatus} of the upload.
     * @throws QuotaExceededException if the upload failed due to quota limitations
     * @throws NullPointerException if {@code timeout} is {@code null}.
     * @throws InterruptedException if the current thread is interrupted while waiting.
     * @throws ExecutionException if the upload completed exceptionally; the cause can be
     *                             retrieved from {@link ExecutionException#getCause()}.
     * @throws TimeoutException if the given timeout elapses before the final status is available.
     */
    public StreamingResourceStatus complete(Duration timeout) throws QuotaExceededException, ExecutionException, InterruptedException, TimeoutException {
        Objects.requireNonNull(timeout);
        session.signalEndOfInput();
        try {
            return session.getFinalStatusFuture().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            // special case: unwrap QuotaExceededException
            if (e.getCause() instanceof QuotaExceededException) {
                throw (QuotaExceededException) e.getCause();
            }
            throw e;
        }
    }


    /**
     * Signals that the upload input is complete and waits indefinitely for the final upload transfer status.
     * <p>
     * This method first calls {@link StreamingUploadSession#signalEndOfInput()} to indicate that
     * the input is complete and no more data will be provided, and then blocks until the final
     * {@link StreamingResourceStatus} becomes available, indicating upload completion.
     *
     * @return the final {@link StreamingResourceStatus} of the upload.
     * @throws QuotaExceededException if the upload failed due to quota limitations
     * @throws InterruptedException if the current thread is interrupted while waiting.
     * @throws ExecutionException if the upload completed exceptionally; the cause can be
     *                             retrieved from {@link ExecutionException#getCause()}.
     *
     * @see #complete(Duration)
     */
    public StreamingResourceStatus complete() throws QuotaExceededException, ExecutionException, InterruptedException {
        session.signalEndOfInput();
        try {
            return session.getFinalStatusFuture().get();
        } catch (ExecutionException e) {
            // special case: unwrap QuotaExceededException
            if (e.getCause() instanceof QuotaExceededException) {
                throw (QuotaExceededException) e.getCause();
            }
            throw e;
        }
    }

    /**
     * Cancels the upload, providing a specific cause for the cancellation.
     * <p>
     * This will signal an exceptional completion to the underlying upload session
     * via {@link StreamingUploadSession#cancel(Exception)}.
     *
     * @param cause the exception that describes the reason for cancellation; must not be null
     */
    public void cancel(Exception cause) {
        session.cancel(Objects.requireNonNull(cause));
    }

    /**
     * Cancels the upload without providing any specific reason.
     * <p>
     * The implementation will internally use a
     * {@link java.util.concurrent.CancellationException} with the message
     * {@code "Cancelled by user"}.
     * @see #cancel(Exception)
     */
    public void cancel() {
        session.cancel(null);
    }

    public String toString() {
        return String.format("StreamingUpload{session=%s}", session);
    }
}
