package step.streaming.common;

import java.util.Objects;

/**
 * Represents the current status of a streaming resource.
 * <p>
 * This includes both the transfer state (e.g., initiated, in-progress, completed, failed)
 * and the known size of the resource at the time the status was generated.
 * @see StreamingResourceTransferStatus
 */
public class StreamingResourceStatus {

    private StreamingResourceTransferStatus transferStatus;
    private Long currentSize;

    /**
     * Default constructor for serialization/deserialization.
     */
    public StreamingResourceStatus() {
    }

    /**
     * Constructs a new {@code StreamingResourceStatus} with the specified transfer status and current size.
     *
     * @param transferStatus the transfer status (must not be null)
     * @param currentSize    the current size of the resource, or {@code null} if transfer failed
     * @throws NullPointerException if {@code transferStatus} is null
     */
    public StreamingResourceStatus(StreamingResourceTransferStatus transferStatus, Long currentSize) {
        this.transferStatus = Objects.requireNonNull(transferStatus, "Transfer status must not be null");
        this.currentSize = currentSize;
    }

    /**
     * Returns the transfer status of the resource.
     *
     * @return the transfer status (never {@code null})
     */
    public StreamingResourceTransferStatus getTransferStatus() {
        return transferStatus;
    }

    /**
     * Sets the transfer status.
     *
     * @param transferStatus the transfer status to set (must not be null)
     * @throws NullPointerException if {@code transferStatus} is null
     */
    public void setTransferStatus(StreamingResourceTransferStatus transferStatus) {
        this.transferStatus = Objects.requireNonNull(transferStatus, "Transfer status must not be null");
    }

    /**
     * Returns the current size of the resource in bytes, or {@code null} if unknown.
     *
     * @return the current size or {@code null}
     */
    public Long getCurrentSize() {
        return currentSize;
    }

    /**
     * Sets the current size of the resource.
     *
     * @param currentSize the size in bytes, or {@code null} if unknown
     */
    public void setCurrentSize(Long currentSize) {
        this.currentSize = currentSize;
    }

    @Override
    public String toString() {
        return "StreamingResourceStatus{" +
                "transferStatus=" + transferStatus +
                ", currentSize=" + currentSize +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof StreamingResourceStatus)) return false;
        StreamingResourceStatus that = (StreamingResourceStatus) o;
        return transferStatus == that.transferStatus &&
                Objects.equals(currentSize, that.currentSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transferStatus, currentSize);
    }
}
