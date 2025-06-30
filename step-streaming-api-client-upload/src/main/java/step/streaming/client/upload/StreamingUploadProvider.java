package step.streaming.client.upload;

import step.streaming.common.StreamingResourceMetadata;

import java.io.File;
import java.io.IOException;

/** An upload provider acts as a factory to create {@link step.streaming.client.upload.StreamingUpload} instances.
 * It is named "provider" instead of "factory" because that less technical term better conveys the intentions.
 */
public interface StreamingUploadProvider {
    /**
     * Starts a streaming upload of a file. The file must exist at invocation time, but it may not be fully written
     * yet, i.e., it may still grow. It is the duty of the caller to properly signal end-of-input, meaning that
     * the file was finished writing.
     * @param fileToStream file to upload
     * @param metadata file metadata, i.e., file name and type
     * @return a {@link StreamingUpload} instance.
     * @throws IOException if the file was not found or another IO error occurred.
     * @see StreamingUpload#signalEndOfInput()
     */
    StreamingUpload startLiveFileUpload(File fileToStream, StreamingResourceMetadata metadata) throws IOException;
}
