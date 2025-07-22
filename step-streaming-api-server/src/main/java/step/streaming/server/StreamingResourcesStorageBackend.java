package step.streaming.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

/**
 * Storage backend used on the server side.
 * The storage is responsible for saving and retrieving actual data content.
 * <p>
 * Storage implementations MUST support regular status updates via callbacks when writing data to indicate progress.
 * The default interval is defined here, but implementations may choose to make it configurable. It is an important
 * setting, as it controls how often incoming data streams will emit events -- which ultimately affects the frequency
 * of status notifications. Note that events should only be emitted if there actually was new data since the last event
 * emission. In other words, size events are never emitted at a faster rate than the interval, but may be less frequent when
 * no data arrives. Line count events (if enabled for a particular resource) should be emitted as soon as they occur, and
 * are expected to be consolidated with the regularly posted size events on the receiving side.
 */

public interface StreamingResourcesStorageBackend {
    /**
     * Default status update notification interval
     */
    long DEFAULT_NOTIFY_INTERVAL_MILLIS = 1000;


    /**
     * Prepares for writing data to a new resource.
     *
     * @param resourceId           internal resource identifier
     * @param enableLineCounting flag to indicate whether line counting must be supported
     * @throws IOException if initialization fails
     */
    void prepareForWrite(String resourceId, boolean enableLineCounting) throws IOException;

    /**
     * Writes data from the input stream and optionally notifies about the growing size, and linebreak count.
     * <p>
     * Note that the linebreakCountConsumer will only be called if linebreak counting is actually enabled for the given resource.
     *
     * @param resourceId             internal resource identifier
     * @param input                  input stream of data to write. This stream MUST be closed at the end of the method.
     * @param fileSizeConsumer       optional listener to receive current size updates (possibly null)
     * @param linebreakCountConsumer optional listener to receive line break count updates (possibly null)
     * @throws IOException if the write fails
     * @see StreamingResourcesStorageBackend#prepareForWrite(String, boolean)
     */
    void writeChunk(String resourceId, InputStream input, Consumer<Long> fileSizeConsumer, Consumer<Long> linebreakCountConsumer) throws IOException;

    /**
     * Opens an input stream to read from a resource in a specific byte range.
     *
     * @param resourceId internal resource identifier
     * @param start      inclusive start byte
     * @param end        exclusive end byte
     * @return input stream for the specified range
     * @throws IOException on access or I/O error
     */
    InputStream openReadStream(String resourceId, long start, long end) throws IOException;

    /**
     * Returns the current number of bytes written to the resource.
     *
     * @param resourceId internal resource identifier
     * @return current size in bytes
     * @throws IOException if retrieval fails
     */
    long getCurrentSize(String resourceId) throws IOException;

    void handleFailedUpload(String resourceId);

    LinebreakIndex getLinebreakIndex(String resourceId) throws IOException;
}
