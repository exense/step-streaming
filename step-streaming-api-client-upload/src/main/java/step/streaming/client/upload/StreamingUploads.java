package step.streaming.client.upload;

import step.streaming.common.StreamingResourceMetadata;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * High-level convenience API for starting streaming uploads using a {@link StreamingUploadProvider}.
 * <p>
 * This class wraps a {@link StreamingUploadProvider} and exposes simplified methods
 * for initiating common types of uploads:
 * <ul>
 *   <li>Text file uploads, using a specified or default character encoding</li>
 *   <li>Binary file uploads</li>
 * </ul>
 * <p>
 * Each method creates the appropriate {@link StreamingResourceMetadata} for the given file
 * and returns a {@link StreamingUpload} object that can be used to signal completion,
 * perform cancellation, and retrieve the final upload status.
 * <p>
 * For more advanced scenarios — such as initiating uploads with custom metadata or MIME types,
 * or directly interacting with the asynchronous behavior of the upload session —
 * use the corresponding lower-level APIs.
 *
 * @see StreamingUploadProvider
 * @see StreamingUploadSession
 */
public class StreamingUploads implements Closeable {

    private final StreamingUploadProvider provider;

    /**
     * Creates a new {@code StreamingUploads} instance backed by the given provider.
     *
     * @param provider the {@link StreamingUploadProvider} to use for starting uploads; must not be {@code null}.
     * @throws NullPointerException if {@code provider} is {@code null}.
     */
    public StreamingUploads(StreamingUploadProvider provider) {
        this.provider = Objects.requireNonNull(provider);
    }

    /**
     * Returns the underlying {@link StreamingUploadProvider}.
     *
     * @return the provider used to start uploads, never {@code null}.
     */
    public StreamingUploadProvider getProvider() {
        return provider;
    }

    /**
     * Starts a streaming upload of a UTF-8 encoded text file.
     * <p>
     * This is a convenience method equivalent to
     * {@link #startTextFileUpload(File, Charset)} with {@link StandardCharsets#UTF_8}.
     *
     * @param textFile the text file to upload.
     * @return a {@link StreamingUpload} representing the active upload.
     * @throws IOException if the file does not exist, cannot be read, or another I/O error occurs.
     */
    public StreamingUpload startTextFileUpload(File textFile) throws IOException {
        return startTextFileUpload(textFile, StandardCharsets.UTF_8);
    }

    /**
     * Starts a streaming upload of a text file with the specified character encoding.
     * <p>
     * The {@link StreamingResourceMetadata} is initialized with:
     * <ul>
     *   <li>the file's name,</li>
     *   <li>{@link StreamingResourceMetadata.CommonMimeTypes#TEXT_PLAIN} as the MIME type,</li>
     *   <li>{@code true} for line access support.</li>
     * </ul>
     * The {@link StreamingUploadProvider#startLiveTextFileUpload(File, StreamingResourceMetadata, Charset)}
     * method is then invoked, and the resulting {@link StreamingUploadSession} is wrapped in a {@link StreamingUpload}.
     *
     * @param textFile the text file to upload.
     * @param charset  the {@link Charset} of the source file; must not be {@code null}.
     * @return a {@link StreamingUpload} representing the active upload.
     * @throws NullPointerException if {@code charset} is {@code null}.
     * @throws IOException if the file does not exist, cannot be read, or another I/O error occurs.
     */
    public StreamingUpload startTextFileUpload(File textFile, Charset charset) throws IOException {
        StreamingUploadSession session = provider.startLiveTextFileUpload(
                textFile,
                new StreamingResourceMetadata(
                        textFile.getName(),
                        StreamingResourceMetadata.CommonMimeTypes.TEXT_PLAIN,
                        true),
                Objects.requireNonNull(charset));
        return new StreamingUpload(session);
    }

    /**
     * Starts a streaming upload of a binary file.
     * <p>
     * The {@link StreamingResourceMetadata} is initialized with:
     * <ul>
     *   <li>the file's name,</li>
     *   <li>{@link StreamingResourceMetadata.CommonMimeTypes#APPLICATION_OCTET_STREAM} as the MIME type,</li>
     *   <li>{@code false} for line access support.</li>
     * </ul>
     * The {@link StreamingUploadProvider#startLiveBinaryFileUpload(File, StreamingResourceMetadata)}
     * method is then invoked, and the resulting {@link StreamingUploadSession} is wrapped in a {@link StreamingUpload}.
     *
     * @param file the binary file to upload.
     * @return a {@link StreamingUpload} representing the active upload.
     * @throws IOException if the file does not exist, cannot be read, or another I/O error occurs.
     */
    public StreamingUpload startBinaryFileUpload(File file) throws IOException {
        StreamingUploadSession session = provider.startLiveBinaryFileUpload(
                file,
                new StreamingResourceMetadata(
                        file.getName(),
                        StreamingResourceMetadata.CommonMimeTypes.APPLICATION_OCTET_STREAM,
                        false));
        return new StreamingUpload(session);
    }

    @Override
    public void close() {
        provider.close();
    }
}
