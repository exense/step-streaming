package step.streaming.server;

import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;

/**
 * Represents an update to the current status of a streaming resource.
 * All fields are nullable, and the semantics is that only non-null fields
 * have actually changed.
 *
 * @see StreamingResourceStatus
 */
public class StreamingResourceStatusUpdate {

    public final StreamingResourceTransferStatus transferStatus;
    public final Long currentSize;
    public final Long numberOfLines;

    public StreamingResourceStatusUpdate(StreamingResourceTransferStatus transferStatus, Long currentSize, Long numberOfLines) {
        this.transferStatus = transferStatus;
        this.currentSize = currentSize;
        this.numberOfLines = numberOfLines;
    }

    public StreamingResourceStatus applyTo(StreamingResourceStatus status) {
        StreamingResourceStatus result = new StreamingResourceStatus(status.getTransferStatus(), status.getCurrentSize(), status.getNumberOfLines());
        if (this.transferStatus != null) {
            result.setTransferStatus(this.transferStatus);
        }
        if (this.currentSize != null) {
            result.setCurrentSize(this.currentSize);
        }
        if (this.numberOfLines != null) {
            result.setNumberOfLines(this.numberOfLines);
        }
        return result;
    }
}
