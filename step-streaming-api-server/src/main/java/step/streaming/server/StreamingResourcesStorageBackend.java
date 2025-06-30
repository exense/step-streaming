package step.streaming.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

/**
 * Storage backend used on the server side.
 * The storage is responsible for saving and retrieving actual data content.
 */
public interface StreamingResourcesStorageBackend {

    /**
     * Prepares for writing data to a new resource.
     *
     * @param resourceId internal resource identifier
     * @throws IOException if initialization fails
     */
    void prepareForWrite(String resourceId) throws IOException;

    /**
     * Writes data from the input stream and optionally notifies about the growing size.
     *
     * @param resourceId    internal resource identifier
     * @param input         input stream of data to write. This stream MUST be closed at the end of the method.
     * @param sizeListener  optional listener to receive current size updates (may be null)
     * @throws IOException if the write fails
     */
    void writeChunk(String resourceId, InputStream input, Consumer<Long> sizeListener) throws IOException;

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
}
