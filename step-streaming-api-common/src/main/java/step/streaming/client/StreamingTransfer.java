package step.streaming.client;

import step.streaming.common.StreamingResourceReference;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Represents an active streaming transfer, either an upload or a download,
 * associated with a specific {@link StreamingResourceReference}.
 * <p>
 * This interface provides access to the current transfer status and supports
 * registering callbacks that are notified on status changes (such as progress
 * updates, completion, or failure).
 */
public interface StreamingTransfer extends AutoCloseable {

    /**
     * Returns the reference associated with this streaming transfer.
     *
     * @return the {@link StreamingResourceReference} used in this transfer
     */
    StreamingResourceReference getReference();

    /**
     * Retrieves the most recently known status of the transfer.
     *
     * @return the current {@link StreamingResourceStatus}
     */
    StreamingResourceStatus getCurrentStatus();

    /**
     * Registers a listener to receive status updates for this transfer.
     * Optionally, updates can be filtered by transfer status types, which
     * may be useful e.g. if you're only interested in FAILED events.
     * If no filter is given, all status updates will be signaled.
     *
     * @param callback                     the listener to notify on status changes
     * @param optionalTransferStatusFilter if provided, the callback will only be invoked
     *                                     for these {@link StreamingResourceTransferStatus} values
     */
    void registerStatusListener(Consumer<StreamingResourceStatus> callback,
                                StreamingResourceTransferStatus... optionalTransferStatusFilter);

    /**
     * Unregisters a previously registered status change listener.
     *
     * @param callback the listener to remove
     */
    void unregisterStatusChangeListener(Consumer<StreamingResourceStatus> callback);

    @Override
    void close() throws IOException;
}
