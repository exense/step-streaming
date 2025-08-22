package step.streaming.client.upload.impl.local;

import step.streaming.client.upload.StreamingUploadSession;
import step.streaming.client.upload.impl.AbstractStreamingUploadProvider;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.data.EndOfInputSignal;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

public class LocalDirectoryBackedStreamingUploadProvider extends AbstractStreamingUploadProvider {
    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("uuuuMMdd_HHmmss");
    private final File localDirectory;

    public LocalDirectoryBackedStreamingUploadProvider(File localDirectory) {
        super(DEFAULT_CONCURRENT_UPLOAD_POOL_SIZE);
        this.localDirectory = localDirectory;
        if (!localDirectory.isDirectory() || !localDirectory.canWrite()) {
            throw new RuntimeException("Directory does not exist or is not writable: " + localDirectory.getAbsolutePath());
        }
    }

    @Override
    protected StreamingUploadSession startLiveFileUpload(InputStream sourceInputStream, StreamingResourceMetadata metadata, EndOfInputSignal endOfInputSignal) throws IOException {
        File outputFile = determineUniqueOutputFile(metadata.getFilename());
        LocalStreamingUploadSession session = new LocalStreamingUploadSession(sourceInputStream, new FileOutputStream(outputFile), metadata, endOfInputSignal);
        executorService.submit(session::transfer);
        return session;
    }

    // Produces filenames from original filename + current time + running number, e.g. 20250819_100434_001_ProcessOut.log
    // synchronized out of an overabundance of caution, but there should not be relevant parallelism...
    private synchronized File determineUniqueOutputFile(String metadataFilename) throws IOException {
        String prefix = LocalDateTime.now().format(timeFormatter);
        String suffix = Objects.requireNonNull(metadataFilename);
        for (int midfix = 1; midfix < 999; ++midfix) { // Hey, I just invented a new word! :-D
            String filename = prefix + String.format("_%03d_", midfix) + suffix;
            File outputFile = new File(localDirectory, filename);
            if (outputFile.createNewFile()) {
                return outputFile;
            }
        }
        throw new IOException("Unable to determine unique local file name for metadata filename " + metadataFilename);
    }
}
