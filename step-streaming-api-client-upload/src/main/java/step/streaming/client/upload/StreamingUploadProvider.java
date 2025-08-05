package step.streaming.client.upload;

import step.streaming.common.StreamingResourceMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

/**
 * An upload provider acts as a factory to create {@link StreamingUploadSession} instances.
 * It is named "provider" instead of "factory" because that less technical term better conveys the intentions.
 */
public interface StreamingUploadProvider {
    /**
     * Starts a streaming upload of a file. The file must exist at invocation time, but it may not be fully written
     * yet, i.e., it may still grow. It is the duty of the caller to properly signal end-of-input, meaning that
     * the file was finished writing.
     * <p>
     * This method should be used when uploading binary data. The file content is sent as-is and not altered.
     * For uploading text files, it is recommended to use the {@link StreamingUploadProvider#startLiveTextFileUpload(File, StreamingResourceMetadata, Charset)}
     * method instead, which ensures that files are normalized to UTF-8 text encoding on upload.
     *
     * @param fileToStream file to upload
     * @param metadata     file metadata, i.e., file name and type
     * @return a {@link StreamingUploadSession} instance.
     * @throws IOException if the file was not found or another IO error occurred.
     * @see StreamingUploadSession#signalEndOfInput()
     * @see StreamingUploadProvider#startLiveTextFileUpload(File, StreamingResourceMetadata, Charset) for uploading textual data
     */
    StreamingUploadSession startLiveBinaryFileUpload(File fileToStream, StreamingResourceMetadata metadata) throws IOException;

    /**
     * Starts a streaming upload of a text file. The file must exist at invocation time, but it may not be fully written
     * yet, i.e., it may still grow. It is the duty of the caller to properly signal end-of-input, meaning that
     * the file was finished writing.
     * <p>
     * This method should be used for uploading text files, and it is the caller's duty to specify the correct encoding
     * of the source file. Implementations MUST convert the file to UTF-8 encoding on the fly before performing the actual upload.
     * All character sets are supported. For multibyte encodings like UTF-16, inlined Byte order markers (BOMs) are
     * supported and taken into consideration when converting.
     *
     * @param textFile file to upload
     * @param metadata file metadata, i.e., file name and type
     * @param charset  the Charset/encoding of the file.
     * @return a {@link StreamingUploadSession} instance.
     * @throws IOException if the file was not found or another IO error occurred.
     * @see StreamingUploadSession#signalEndOfInput()
     * @see StreamingUploadProvider#startLiveBinaryFileUpload(File, StreamingResourceMetadata) for uploading binary files.
     */
    StreamingUploadSession startLiveTextFileUpload(File textFile, StreamingResourceMetadata metadata, Charset charset) throws IOException;

}
