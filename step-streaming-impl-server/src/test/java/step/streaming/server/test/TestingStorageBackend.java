package step.streaming.server.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.server.FilesystemStreamingResourcesStorageBackend;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A test-friendly subclass of {@link FilesystemStreamingResourcesStorageBackend} that uses a temporary directory
 * and provides a cleanup method for deleting test data after use.
 */
public class TestingStorageBackend extends FilesystemStreamingResourcesStorageBackend {
    private static final Logger logger = LoggerFactory.getLogger(TestingStorageBackend.class);

    private final File tempDirectory;

    /**
     * Constructs a new testing backend with a unique temporary directory.
     *
     */
    public TestingStorageBackend(Long flushInterval, boolean paranoidSyncMode) {
        super(createTempDirectory(), true, flushInterval, paranoidSyncMode);
        this.tempDirectory = super.baseDirectory;
    }

    private static File createTempDirectory() {
        try {
            Path tempPath = Files.createTempDirectory("streaming-storage-test-");
            logger.info("Created temporary directory: {}", tempPath);
            return tempPath.toFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Recursively deletes the temporary directory and all of its contents.
     *
     */
    public void cleanup() {
        try {
            deleteRecursively(tempDirectory.toPath());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteRecursively(Path path) throws IOException {
        if (!Files.exists(path)) return;

        Files.walk(path)
                .sorted((a, b) -> b.compareTo(a)) // delete children before parents
                .forEach(p -> {
                    try {
                        logger.debug("Cleaning up: deleting {} {}", p.toFile().isDirectory() ? "directory": "file", p);
                        Files.delete(p);
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to delete: " + p, e);
                    }
                });
    }

    /**
     * Returns the root temp directory used for storage.
     */
    public File getTempDirectory() {
        return tempDirectory;
    }
}
