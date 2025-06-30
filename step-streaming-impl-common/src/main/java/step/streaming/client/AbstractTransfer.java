package step.streaming.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.StreamingResourceReference;
import step.streaming.common.StreamingResourceStatus;
import step.streaming.common.StreamingResourceTransferStatus;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Base implementation of the {@link StreamingTransfer} interface, providing common functionality
 * for managing transfer state and notifying listeners of status changes.
 * <p>
 * This class handles registration and notification of listeners that are interested in updates
 * to a resource's transfer status. Emitters should invoke {@link #setCurrentStatus(StreamingResourceStatus)}
 * when the transfer status changes, which will automatically propagate updates to all registered listeners.
 * </p>
 * <p>
 * The associated {@link StreamingResourceReference} must be set via {@link #setReference(StreamingResourceReference)}
 * before use.
 * </p>
 */
public abstract class AbstractTransfer implements StreamingTransfer {
    private static final Logger logger = LoggerFactory.getLogger(AbstractTransfer.class);
    private StreamingResourceStatus currentStatus;
    private StreamingResourceReference reference;
    private final Map<Consumer<StreamingResourceStatus>, List<StreamingResourceTransferStatus>> statusListeners = new ConcurrentHashMap<>();

    protected AbstractTransfer() {
    }

    protected void setReference(StreamingResourceReference reference) {
        this.reference = Objects.requireNonNull(reference);
    }

    @Override
    public StreamingResourceReference getReference() {
        return reference;
    }

    @Override
    public StreamingResourceStatus getCurrentStatus() {
        return currentStatus;
    }

    /**
     * Updates the current status and notifies all registered listeners whose status filters
     * match the new transfer status (or who have not specified a filter).
     *
     * @param status the new transfer status; must not be {@code null}
     */
    protected void setCurrentStatus(StreamingResourceStatus status) {
        currentStatus = Objects.requireNonNull(status);
        for (Map.Entry<Consumer<StreamingResourceStatus>, List<StreamingResourceTransferStatus>> callback : statusListeners.entrySet()) {
            if (callback.getValue().isEmpty() || callback.getValue().contains(status.getTransferStatus())) {
                try {
                    callback.getKey().accept(status);
                } catch (Exception e) {
                    logger.error("Status callback failed", e);
                }
            }
        }
    }

    @Override
    public void registerStatusListener(Consumer<StreamingResourceStatus> callback, StreamingResourceTransferStatus... optionalTransferStatusFilter) {
        statusListeners.put(Objects.requireNonNull(callback), Arrays.asList(Objects.requireNonNull(optionalTransferStatusFilter)));
    }

    @Override
    public void unregisterStatusChangeListener(Consumer<StreamingResourceStatus> callback) {
        statusListeners.remove(Objects.requireNonNull(callback));
    }

}
