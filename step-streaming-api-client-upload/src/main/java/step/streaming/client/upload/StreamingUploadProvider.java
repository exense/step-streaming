package step.streaming.client.upload;

import step.streaming.common.QuotaExceededException;
import step.streaming.common.StreamingResourceMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;


/**
 * A provider for creating {@link StreamingUploadSession} instances.
 * <p>
 * This interface acts as a factory for starting streaming uploads of files,
 * either as raw binary data or as text data that is converted to UTF-8 during upload.
 * It is named "provider" instead of "factory" to convey a less technical,
 * more user-oriented concept.
 */
public interface StreamingUploadProvider {

    /**
     * Starts a streaming upload of a binary file.
     * <p>
     * The file must exist at the time this method is invoked, but it may still be
     * growing (i.e., being written to) during the upload process. It is the caller's
     * responsibility to signal the end of input via
     * {@link StreamingUploadSession#signalEndOfInput()} once the file has been
     * completely written.
     * <p>
     * This method should be used for uploading non-text data such as images, videos,
     * archives, or any other binary content. The file content is sent as-is without
     * any re-encoding or transformation.
     * <p>
     * For uploading text files, prefer
     * {@link #startLiveTextFileUpload(File, StreamingResourceMetadata, Charset)},
     * which ensures that text content is normalized to UTF-8 encoding during upload.
     *
     * @param fileToStream the file to upload.
     * @param metadata     the metadata describing the file, such as file name and content type.
     * @return a {@link StreamingUploadSession} instance representing the active upload.
     * @throws QuotaExceededException if the upload failed due to quota limitations
     * @throws IOException            if the file does not exist, cannot be read, or another I/O error occurs.
     * @see StreamingUploadSession#signalEndOfInput()
     * @see #startLiveTextFileUpload(File, StreamingResourceMetadata, Charset)
     */
    StreamingUploadSession startLiveBinaryFileUpload(File fileToStream, StreamingResourceMetadata metadata) throws QuotaExceededException, IOException;

    /**
     * Starts a streaming upload of a text file.
     * <p>
     * The file must exist when this method is invoked, but it may still be
     * growing (i.e., being written to) during the upload. It is the caller's
     * responsibility to signal the end of input via
     * {@link StreamingUploadSession#signalEndOfInput()} once the file has been
     * fully written.
     * <p>
     * This method is intended for uploading text files. Callers must provide the
     * correct {@link Charset} that reflects the file's current encoding.
     * Implementations will convert the content to UTF-8 encoding on the fly before
     * uploading. All character sets are supported. For multibyte encodings such
     * as UTF-16, embedded byte order markers (BOMs) are supported and will be
     * taken into account (and then removed) during conversion.
     * <p>
     * To enable the full range of server-side features — in particular, retrieving
     * the file’s content by line number — ensure that the provided
     * {@link StreamingResourceMetadata} is configured accordingly, e.g. by calling
     * {@link StreamingResourceMetadata#setSupportsLineAccess(boolean)} with a
     * value of {@code true}.
     *
     * @param textFile the text file to upload.
     * @param metadata the metadata describing the file, such as file name and content type.
     * @param charset  the {@link Charset} (encoding) of the source file; must not be {@code null}.
     * @return a {@link StreamingUploadSession} instance representing the active upload.
     * @throws QuotaExceededException if the upload failed due to quota limitations
     * @throws IOException            if the file does not exist, cannot be read, or another I/O error occurs.
     * @see StreamingUploadSession#signalEndOfInput()
     * @see #startLiveBinaryFileUpload(File, StreamingResourceMetadata)
     */
    StreamingUploadSession startLiveTextFileUpload(File textFile, StreamingResourceMetadata metadata, Charset charset) throws QuotaExceededException, IOException;

    /**
     * Closing a provider means cancelling any potential ongoing uploads.
     * This method must not throw exceptions.
     */
    void close();
}
