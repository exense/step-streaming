package step.streaming.client.upload.impl;

import step.streaming.client.upload.StreamingUploadProvider;
import step.streaming.client.upload.StreamingUploadSession;
import step.streaming.common.QuotaExceededException;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.data.LimitedBufferInputStream;
import step.streaming.data.EndOfInputSignal;
import step.streaming.data.LiveFileInputStream;
import step.streaming.data.UTF8TranscodingTextInputStream;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Objects;
import java.util.concurrent.*;

/**
 * Abstract base class for implementing a {@link StreamingUploadProvider}.
 * <p>
 * This class provides shared logic for initiating live streaming uploads from
 * either binary or text files. It handles file polling, buffering, optional character
 * set conversion for text content, and the creation of appropriate input stream wrappers.
 * <p>
 * Subclasses must implement {@link #startLiveFileUpload(InputStream, StreamingResourceMetadata, EndOfInputSignal)},
 * which is responsible for initiating the actual upload transfer.
 */
@SuppressWarnings("unused")
public abstract class AbstractStreamingUploadProvider implements StreamingUploadProvider {

    /**
     * Default interval (in milliseconds) between file size checks during a live upload.
     */
    public static final long DEFAULT_FILE_POLL_INTERVAL_MS = 100;

    /**
     * Default buffer size (in bytes) for limiting upload buffering.
     * This is intentionally small, because excessive buffering could delay uploads
     * if the data is coming in slowly and in small increments, such as log files.
     */
    public static final int DEFAULT_BUFFER_SIZE = 64;

    /**
     * Default thread pool size used for executing concurrent uploads.
     */
    public static final int DEFAULT_CONCURRENT_UPLOAD_POOL_SIZE = 100;

    /**
     * Interval (in milliseconds) between polling operations when reading from a growing file.
     */
    protected long uploadFilePollInterval = DEFAULT_FILE_POLL_INTERVAL_MS;

    /**
     * Input buffer size limit.
     */
    protected int uploadBufferSize = DEFAULT_BUFFER_SIZE;

    /**
     * Executor service used to process uploads concurrently.
     */
    protected final ExecutorService executorService;

    /**
     * Creates a new instance with a fixed-size thread pool for concurrent uploads.
     *
     * @param uploadThreadPoolSize the number of concurrent upload threads to allow.
     */
    public AbstractStreamingUploadProvider(int uploadThreadPoolSize) {
        if (uploadThreadPoolSize <= 0) {
            throw new IllegalArgumentException("Upload thread pool size must be greater than zero.");
        }
        executorService = Executors.newFixedThreadPool(uploadThreadPoolSize);
    }

    /**
     * Returns the current file polling interval in milliseconds.
     * This value determines how frequently the system checks for new file content
     * during a live upload.
     *
     * @return the current polling interval in milliseconds.
     */
    public long getUploadFilePollInterval() {
        return uploadFilePollInterval;
    }

    /**
     * Sets the polling interval used when uploading from a growing file.
     *
     * @param uploadFilePollInterval the interval in milliseconds; must be &gt; 0.
     * @throws IllegalArgumentException if the given interval is not positive.
     */
    public void setUploadFilePollInterval(long uploadFilePollInterval) {
        if (uploadFilePollInterval <= 0) {
            throw new IllegalArgumentException("uploadFilePollInterval must be greater than 0");
        }
        this.uploadFilePollInterval = uploadFilePollInterval;
    }

    /**
     * Returns the current upload buffer size in bytes.
     *
     * @return the current buffer size.
     */
    public int getUploadBufferSize() {
        return uploadBufferSize;
    }

    /**
     * Sets the buffer size used to limit upload buffering
     *
     * @param uploadBufferSize the buffer size in bytes; must be &gt; 0.
     * @throws IllegalArgumentException if the given size is not positive.
     */
    public void setUploadBufferSize(int uploadBufferSize) {
        if (uploadBufferSize <= 0) {
            throw new IllegalArgumentException("uploadFileBufferSize must be greater than 0");
        }
        this.uploadBufferSize = uploadBufferSize;
    }

    private final CopyOnWriteArrayList<StreamingUploadSession> activeSessions = new CopyOnWriteArrayList<>();

    // once closing, no more uploads will be accepted, otherwise no relevant limits.
    private static final int ALL_PERMITS = Integer.MAX_VALUE;
    private final Semaphore admissionSemaphore = new Semaphore(ALL_PERMITS, false);

    /**
     * Starts a live upload of a binary file.
     * <p>
     * This method wraps the file in a {@link LiveFileInputStream} and throttles
     * reads using a {@link LimitedBufferInputStream}. Data is passed through unchanged.
     *
     * @param fileToStream the binary file to upload.
     * @param metadata     metadata describing the file to upload.
     * @return a {@link StreamingUploadSession} representing the active upload.
     * @throws QuotaExceededException if the server signals that the upload failed due to quota limitations
     * @throws IOException if the file cannot be read or an error occurs during stream setup.
     */
    @Override
    public StreamingUploadSession startLiveBinaryFileUpload(File fileToStream, StreamingResourceMetadata metadata) throws QuotaExceededException, IOException {
        return startLiveFileUpload(fileToStream, metadata, null);
    }

    /**
     * Starts a live upload of a text file, converting its content to UTF-8 encoding if needed.
     *
     * @param textFile the text file to upload.
     * @param metadata metadata describing the file to upload.
     * @param charset  the character encoding of the source file.
     * @return a {@link StreamingUploadSession} representing the active upload.
     * @throws QuotaExceededException if the upload failed due to quota limitations
     * @throws IOException if the file cannot be read or an error occurs during stream setup.
     */
    @Override
    public StreamingUploadSession startLiveTextFileUpload(File textFile, StreamingResourceMetadata metadata, Charset charset) throws QuotaExceededException, IOException {
        return startLiveFileUpload(textFile, metadata, charset);
    }

    /**
     * Shared internal method for starting a file upload, optionally converting the content
     * from a source character set to UTF-8.
     *
     * @param fileToStream       the file to stream.
     * @param metadata           metadata describing the file.
     * @param convertFromCharset if non-null, the character set used to decode the file before converting to UTF-8.
     * @return a {@link StreamingUploadSession} representing the active upload.
     * @throws QuotaExceededException if the upload failed due to quota limitations
     * @throws IOException if the file cannot be read or the stream cannot be created.
     */
    protected StreamingUploadSession startLiveFileUpload(File fileToStream, StreamingResourceMetadata metadata, Charset convertFromCharset) throws QuotaExceededException, IOException {
        if (!admissionSemaphore.tryAcquire()) {
            throw new IOException("Upload provider is closed, not accepting new uploads");
        }
        try {
            Objects.requireNonNull(fileToStream);
            Objects.requireNonNull(metadata);
            EndOfInputSignal endOfInputSignal = new EndOfInputSignal();
            LiveFileInputStream liveInputStream = new LiveFileInputStream(fileToStream, endOfInputSignal, uploadFilePollInterval);
            InputStream uploadInputStream = convertFromCharset == null
                    ? liveInputStream
                    : new UTF8TranscodingTextInputStream(liveInputStream, convertFromCharset);
            InputStream limitedBufferInputStream = new LimitedBufferInputStream(uploadInputStream, uploadBufferSize);
            StreamingUploadSession session = startLiveFileUpload(limitedBufferInputStream, metadata, endOfInputSignal);
            session.onClose(ignoredMessage -> activeSessions.remove(session));
            activeSessions.add(session);
            return session;
        } finally {
            admissionSemaphore.release();
        }
    }

    /**
     * Abstract method to be implemented by subclasses to initiate the actual upload
     * using the provided input stream, metadata, and end-of-input signal.
     * <p>
     * Unless the calling method is overridden, the input stream will be a {@link LimitedBufferInputStream} wrapping
     * a {@link LiveFileInputStream}, using the poll interval and buffer size the provider is configured with.
     *
     * @param sourceInputStream the input stream representing the upload content.
     * @param metadata          the metadata describing the resource.
     * @param endOfInputSignal  the signal used to detect upload completion or cancellation.
     * @throws QuotaExceededException if the upload failed due to quota limitations
     * @return a {@link StreamingUploadSession} representing the active upload.
     * @throws IOException if the stream setup or transfer initiation fails.
     */
    protected abstract StreamingUploadSession startLiveFileUpload(InputStream sourceInputStream,
                                                                  StreamingResourceMetadata metadata,
                                                                  EndOfInputSignal endOfInputSignal) throws QuotaExceededException, IOException;

    @Override
    public void close() {
        // Acquire all "upload permits" and never release them.
        // If an upload is currently being started (which would be aborted anyway right away by the code below),
        // this will wait for the instantiation to complete, followed by blocking any new uploads by fully "using"
        // the semaphore. Note that this is a highly improbable scenario: we don't actually expect
        // any uploads to be performed at the stage when we're closing, but you never know what weird ways to use things
        // people might invent :-D - so this is an effective and performant lock-free solution.
        admissionSemaphore.acquireUninterruptibly(ALL_PERMITS);

        // Close any remaining sessions. This will abort any session that has not been properly
        // closed before, while being a no-op if it was already closed -- so potential race conditions don't
        // really matter here.
        for (StreamingUploadSession session : activeSessions) {
            try {
                session.close();
            } catch (IOException e) {
                // this should never happen, and if it does - well, we're aborting the upload anyway.
            }
        }
    }
}
