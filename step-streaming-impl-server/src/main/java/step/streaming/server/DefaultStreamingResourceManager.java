package step.streaming.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.common.*;
import step.streaming.server.data.LineSlicingIterator;
import step.streaming.util.ThrowingConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class DefaultStreamingResourceManager implements StreamingResourceManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultStreamingResourceManager.class);

    protected final StreamingResourcesCatalogBackend catalog;
    protected final StreamingResourcesStorageBackend storage;
    protected final StreamingResourceUploadContexts uploadContexts;
    protected final Function<String, StreamingResourceReference> referenceProducerFunction;
    protected final ExecutorService uploadsThreadPool;

    // Listeners interested in a single resource (e.g. download clients). Listeners interested in an entire context will use the respective methods in the uploadContexts instead.
    protected final Map<String, CopyOnWriteArrayList<Consumer<StreamingResourceStatus>>> statusListeners = new ConcurrentHashMap<>();

    public DefaultStreamingResourceManager(StreamingResourcesCatalogBackend catalog,
                                           StreamingResourcesStorageBackend storage,
                                           Function<String, StreamingResourceReference> referenceProducerFunction,
                                           StreamingResourceUploadContexts uploadContexts,
                                           ExecutorService uploadsThreadPool

    ) {
        this.catalog = Objects.requireNonNull(catalog);
        this.storage = Objects.requireNonNull(storage);
        this.referenceProducerFunction = Objects.requireNonNull(referenceProducerFunction);
        this.uploadContexts = uploadContexts;
        this.uploadsThreadPool = uploadsThreadPool;
    }

    @Override
    public ExecutorService getUploadsThreadPool() {
        return uploadsThreadPool;
    }

    @Override
    public StreamingResourceReference getReferenceFor(String resourceId) {
        return referenceProducerFunction.apply(resourceId);
    }

    @Override
    public String registerNewResource(StreamingResourceMetadata metadata, String uploadContextId) throws QuotaExceededException, IOException {
        StreamingResourceUploadContext uploadContext = (uploadContextId != null && uploadContexts != null) ? uploadContexts.getContext(uploadContextId) : null;
        String resourceId = catalog.createResource(metadata, uploadContext);
        logger.debug("Created new streaming resource: {}, context={}", resourceId, uploadContextId);

        try {
            storage.prepareForWrite(resourceId, metadata.getSupportsLineAccess());
        } catch (IOException e) {
            markFailed(resourceId);
            logger.error("Storage backend failed to prepare resource {} for writing", resourceId, e);
            throw e;
        }
        if (uploadContext != null) {
            uploadContexts.onResourceCreated(uploadContextId, resourceId, metadata);
        }
        StreamingResourceStatusUpdate update = new StreamingResourceStatusUpdate(StreamingResourceTransferStatus.INITIATED, 0L, metadata.getSupportsLineAccess() ? 0L : null);
        StreamingResourceStatus status = catalog.updateStatus(resourceId, update);
        emitStatus(resourceId, status);

        return resourceId;
    }

    /**
     * Invoked during the upload of a resource whenever a change in size is published.
     * This is useful for enforcing quotas etc.
     * The default implementation does nothing; override in subclasses if needed.
     *
     * @param resourceId  the affected resource
     * @param currentSize the last known size of the resource
     * @throws QuotaExceededException to signal specifically that quota was exceeded. This will effectively abort the upload and set it as FAILED.
     * @throws IOException to signal any other I/O error. This will effectively abort the upload and set it as FAILED.
     */
    protected void onSizeChanged(String resourceId, long currentSize) throws QuotaExceededException, IOException {

    }

    @Override
    public long writeChunk(String resourceId, InputStream input) throws IOException {
        try {
            long sizeBeforeChunk = storage.getCurrentSize(resourceId);
            AtomicReference<Long> linebreakCount = new AtomicReference<>();
            ThrowingConsumer<Long> linebreakCountListener = linebreakCount::set;
            ThrowingConsumer<Long> sizeListener = chunkSize -> {
                long updatedSize = sizeBeforeChunk + chunkSize;
                onSizeChanged(resourceId, updatedSize);
                StreamingResourceStatusUpdate update = new StreamingResourceStatusUpdate(
                        StreamingResourceTransferStatus.IN_PROGRESS, updatedSize, linebreakCount.get()
                );
                logger.debug("Updating streaming resource: {}, statusUpdate={}", resourceId, update);
                StreamingResourceStatus status = catalog.updateStatus(resourceId, update);
                logger.debug("Updated streaming resource: {}, status={}", resourceId, status);
                emitStatus(resourceId, status);
            };
            storage.writeChunk(resourceId, input, sizeListener, linebreakCountListener);

            long currentSize = storage.getCurrentSize(resourceId);
            logger.debug("Delegated chunk write for {} (current size: {})", resourceId, currentSize);
            return currentSize;
        } catch (IOException e) {
            logger.warn("IOException during writeChunk for {} — marking FAILED: {}", resourceId, e.getMessage());
            markFailed(resourceId);
            throw e;
        }
    }

    private StreamingResourceStatusUpdate getFinalStatusUpdate(String resourceId, StreamingResourceTransferStatus transferStatus) throws IOException {
        long finalSize = storage.getCurrentSize(resourceId);
        LinebreakIndex linebreakIndex = storage.getLinebreakIndex(resourceId);
        Long correctedNumberOfLines = null;
        // Line numbers are a PITA in some edge cases, because not all files properly end with a linebreak.
        // Note that this only (potentially) concerns the very last line of the file. If the last byte
        // in the file is a linebreak, all is good -- the file is properly terminated.
        // However, if it is NOT, then the last line will span from the position of the last LB+1 to the end of the file.
        // We only enter this block at all if an index is present (i.e. line indexing is on)
        if (linebreakIndex != null) {
            long linebreakCount = linebreakIndex.getTotalEntries();
            if (linebreakCount > 0) {
                long lastLb = linebreakIndex.getLinebreakPosition(linebreakCount - 1);
                if (lastLb != finalSize - 1) {
                    correctedNumberOfLines = linebreakCount + 1;
                }
            } else {
                // even more exotic: no linebreak at all -> single line, UNLESS the file has 0 bytes.
                correctedNumberOfLines = finalSize > 0 ? 1L : 0L;
            }
        }
        return new StreamingResourceStatusUpdate(transferStatus, finalSize, correctedNumberOfLines);
    }

    @Override
    public void markCompleted(String resourceId) {
        try {
            StreamingResourceStatusUpdate update = getFinalStatusUpdate(resourceId, StreamingResourceTransferStatus.COMPLETED);
            StreamingResourceStatus status = catalog.updateStatus(resourceId, update);
            logger.debug("Resource marked COMPLETED: {}, status={}", resourceId, status);
            emitStatus(resourceId, status);
        } catch (IOException e) {
            logger.warn("IOException during markCompleted for {}, marking as failed instead", resourceId, e);
            markFailed(resourceId);
        }
    }

    @Override
    public void markFailed(String resourceId) {
        boolean stillExists = storage.handleFailedUpload(resourceId);
        StreamingResourceStatusUpdate update;
        if (stillExists) {
            // update line numbers
            try {
                update = getFinalStatusUpdate(resourceId, StreamingResourceTransferStatus.FAILED);
            } catch (IOException e) {
                // should never happen, but if it does, only update the status itself, not the sizes
                logger.error("Unexpected error while updating status for failed upload, resourceId={}", resourceId, e);
                update = new StreamingResourceStatusUpdate(StreamingResourceTransferStatus.FAILED, null, null);
            }
        } else {
            // file was deleted, also update size
            update = new StreamingResourceStatusUpdate(StreamingResourceTransferStatus.FAILED, 0L, 0L);
        }
        StreamingResourceStatus status = catalog.updateStatus(resourceId, update);
        logger.warn("Resource marked FAILED: {}, status={}", resourceId, status);
        emitStatus(resourceId, status);
    }

    @Override
    public StreamingResourceStatus getStatus(String resourceId) {
        return catalog.getStatus(resourceId);
    }

    @Override
    public InputStream openStream(String resourceId, long start, long end) throws IOException {
        logger.debug("Opening stream for {}, chunk [{}, {}]", resourceId, start, end);
        return storage.openReadStream(resourceId, start, end);
    }

    @Override
    public void registerStatusListener(String resourceId, Consumer<StreamingResourceStatus> listener) {
        statusListeners
                .computeIfAbsent(resourceId, k -> new CopyOnWriteArrayList<>())
                .add(listener);

        logger.debug("Registered status listener for {}", resourceId);

        StreamingResourceStatus currentStatus = catalog.getStatus(resourceId);
        if (currentStatus != null) {
            listener.accept(currentStatus);
        }
    }

    @Override
    public void unregisterStatusListener(String resourceId, Consumer<StreamingResourceStatus> listener) {
        var listeners = statusListeners.get(resourceId);
        if (listeners != null) {
            if (listeners.remove(listener)) {
                logger.debug("Unregistered status listener for {}", resourceId);
            }
            if (listeners.isEmpty()) {
                statusListeners.remove(resourceId);
            }
        }
    }

    private void emitStatus(String resourceId, StreamingResourceStatus status) {
        var listeners = statusListeners.get(resourceId);
        if (listeners != null) {
            logger.debug("Emitting status update to {} listener(s) for {}: {}", listeners.size(), resourceId, status);
            for (var listener : listeners) {
                try {
                    listener.accept(status);
                } catch (Exception e) {
                    logger.error("Status listener unexpectedly threw exception {} on resource {}, status {}", listener, resourceId, status, e);
                }
            }
        }
        if (uploadContexts != null) {
            String uploadContextId = uploadContexts.getContextIdForResourceId(resourceId);
            if (uploadContextId != null) {
                uploadContexts.onResourceStatusChanged(uploadContextId, resourceId, status);
            }
        }
    }

    @Override
    public Stream<Long> getLinebreakPositions(String resourceId, long startingLinebreakIndex, long count) throws IOException {
        // this will throw an IllegalArgumentException if the ID does not exist
        StreamingResourceStatus status = catalog.getStatus(resourceId);
        if (status.getNumberOfLines() == null) {
            throw new IllegalArgumentException("Resource " + resourceId + " does not support access by line number");
        }
        LinebreakIndex index = storage.getLinebreakIndex(resourceId);
        if (index == null) {
            throw new IllegalStateException("Linebreak index not found for resource " + resourceId);
        }
        if (count < 0) {
            throw new IllegalArgumentException("count must not be negative");
        }
        // edge case -> return an empty stream when count is 0, regardless of requested index.
        if (count == 0) {
            return Stream.empty();
        }
        if (startingLinebreakIndex < 0 || startingLinebreakIndex >= status.getNumberOfLines()) {
            throw new IndexOutOfBoundsException("Linebreak index out of bounds: " + startingLinebreakIndex + "; acceptable values: [0, " + status.getNumberOfLines() + "[");
        }
        long lastIndex = startingLinebreakIndex + count;
        if (lastIndex > status.getNumberOfLines()) {
            throw new IndexOutOfBoundsException("Starting index + count exceeds number of entries: " + startingLinebreakIndex + " + " + count + " > " + status.getNumberOfLines());
        }
        // special case for the last (logical) linebreak, which may not be present in the index, but actually be EOF
        if (lastIndex == status.getNumberOfLines()) {
            if (index.getTotalEntries() == lastIndex) {
                // Index DOES have the last linebreak (i.e. file ends with LB)
                return index.getLinebreakPositions(startingLinebreakIndex, count);
            } else {
                // Index DOES NOT have last LB -- append it to the end of the stream by determining from file size.
                // Note that the last byte offset is the file size - 1.
                return Stream.concat(index.getLinebreakPositions(startingLinebreakIndex, count - 1), Stream.of(status.getCurrentSize() - 1));
            }
        } else {
            return index.getLinebreakPositions(startingLinebreakIndex, count);
        }
    }

    @Override
    public Stream<String> getLines(String resourceId, long startingLineIndex, long count) throws IOException {
        // this will perform any required validation. However, it returns the positions where the lines END, not where they start.
        Stream<Long> linebreakPositions = getLinebreakPositions(resourceId, startingLineIndex, count);
        // edge case -> return empty stream immediately
        if (count == 0) {
            return Stream.empty();
        }
        // We can't look at the last element of the stream without consuming it, so we just perform a new request just for the last LB.
        // This will produce a stream with one element (last byte position), and we need to add 1 because the read requests will stop *before* the given argument.
        long lastByteExclusive = getLinebreakPositions(resourceId, startingLineIndex + count - 1, 1).collect(Collectors.toList()).get(0) + 1;
        // The first byte is either the start of the file, or the position of the linebreak *prior* to the requested line (plus 1 to skip the LB itself)
        long firstByteInclusive = (startingLineIndex > 0) ? storage.getLinebreakIndex(resourceId).getLinebreakPosition(startingLineIndex - 1) + 1 : 0;

        InputStream bytesStream = openStream(resourceId, firstByteInclusive, lastByteExclusive);

        PrimitiveIterator.OfLong relativeLinebreakPositions = linebreakPositions.mapToLong(p -> p - firstByteInclusive).iterator();
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(new LineSlicingIterator(bytesStream, relativeLinebreakPositions),
                        Spliterator.ORDERED | Spliterator.NONNULL),
                false).onClose(() -> {
            try {
                bytesStream.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    @Override
    public void deleteResource(String resourceId) throws IOException {
        catalog.delete(resourceId);
        storage.delete(resourceId);
    }

}
