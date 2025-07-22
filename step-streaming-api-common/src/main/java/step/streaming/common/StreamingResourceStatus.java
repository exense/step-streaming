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
    private long currentSize;
    private Long numberOfLines;

    /**
     * Default constructor for serialization/deserialization.
     */
    public StreamingResourceStatus() {
    }

    /**
     * Constructs a new {@code StreamingResourceStatus} with the specified transfer status and size/ lines information.
     *
     * @param transferStatus the transfer status (must not be null)
     * @param currentSize    the current size of the resource
     * @param numberOfLines    the current number of lines, if applicable
     * @throws NullPointerException if {@code transferStatus} is null
     */
    public StreamingResourceStatus(StreamingResourceTransferStatus transferStatus, long currentSize, Long numberOfLines) {
        this.transferStatus = Objects.requireNonNull(transferStatus, "Transfer status must not be null");
        this.currentSize = currentSize;
        this.numberOfLines = numberOfLines;
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
     * Returns the current size of the resource in bytes
     *
     * @return the current size
     */
    public Long getCurrentSize() {
        return currentSize;
    }

    /**
     * Sets the current size of the resource.
     *
     * @param currentSize the size in bytes
     */
    public void setCurrentSize(long currentSize) {
        this.currentSize = currentSize;
    }

    /**
     * Returns the current lines count, or null if not applicable
     *
     * @return the lines count (could be null)
     */
    public Long getNumberOfLines() {
        return numberOfLines;
    }

    /**
     * Sets the current lines count of the resource
     *
     * @param numberOfLines the number of lines
     */
    public void setNumberOfLines(Long numberOfLines) {
        this.numberOfLines = numberOfLines;
    }

    @Override
    public String toString() {
        return "StreamingResourceStatus{" +
                "transferStatus=" + transferStatus +
                ", currentSize=" + currentSize +
                ", numberOfLines=" + numberOfLines +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof StreamingResourceStatus)) return false;
        StreamingResourceStatus that = (StreamingResourceStatus) o;
        return currentSize == that.currentSize && transferStatus == that.transferStatus && Objects.equals(numberOfLines, that.numberOfLines);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transferStatus, currentSize, numberOfLines);
    }
}
