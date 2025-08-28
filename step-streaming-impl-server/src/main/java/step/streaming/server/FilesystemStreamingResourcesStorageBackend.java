package step.streaming.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.data.CheckpointingOutputStream;
import step.streaming.data.FileChunk;
import step.streaming.util.ThrowingConsumer;
import step.streaming.data.LinebreakDetectingOutputStream;
import step.streaming.server.data.LinebreakIndexFile;

import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.function.Function;

/**
 * A file-based implementation of {@link StreamingResourcesStorageBackend} that stores each resource
 * in a dedicated file, separated into 2-level subdirectories for better scalability. If resource IDs tend
 * to have the same prefix (first 4 characters), they should be hashed for better distribution to directories.
 * The one-argument constructor hashes by default. If IDs are already well-distributed, this may be turned off
 * so the directory hierarchy better reflects the actual IDs (and omits the minimal hashing overhead).
 * Functionally, there is no difference though.
 * <p>
 * A note on paranoid mode: this enables synchronous writes, where every write is synced to the filesystem immediately.
 * Something like that may be required in Databases for absolute consistency, but it's much less relevant here
 * as long as the final write is guaranteed to sync to the file system.
 * <p>
 * I had initially enabled it (without the flag), only to wonder why everything was extremely slow.
 * From the full test suite, where a file is uploaded and downloaded in parallel, then downloaded again: For a 5GB
 * scenario (thus 15GB total transfers), this takes around 30 seconds in non-paranoid mode, but 7 HOURS in paranoid mode.
 * <p>
 * We're still providing the flag, but consider it useful only in smaller test scenarios.
 */
public class FilesystemStreamingResourcesStorageBackend implements StreamingResourcesStorageBackend {
    private static final Logger logger = LoggerFactory.getLogger(FilesystemStreamingResourcesStorageBackend.class);

    protected final File baseDirectory;
    protected final boolean hashIdsBeforeStoring;
    protected final long flushAndNotifyIntervalMillis;
    protected final boolean paranoidSyncMode;

    public FilesystemStreamingResourcesStorageBackend(File baseDirectory) {
        this(baseDirectory, true);
    }

    public FilesystemStreamingResourcesStorageBackend(File baseDirectory, boolean hashIdsBeforeStoring) {
        this(baseDirectory, hashIdsBeforeStoring, DEFAULT_NOTIFY_INTERVAL_MILLIS, false);
    }

    /**
     * Creates a new backend with the given configuration.
     *
     * @param baseDirectory                the root directory for storage
     * @param hashIdsBeforeStoring         whether to hash resource IDs for directory distribution/scalability
     * @param flushAndNotifyIntervalMillis interval in which data is flushed to storage, and events emitted
     * @param paranoidSyncMode             use paranoid sync (warning: not recommended as it's extremely slow!)
     */
    public FilesystemStreamingResourcesStorageBackend(File baseDirectory, boolean hashIdsBeforeStoring, long flushAndNotifyIntervalMillis, boolean paranoidSyncMode) {
        this.baseDirectory = validateBaseDirectory(Objects.requireNonNull(baseDirectory));
        this.hashIdsBeforeStoring = hashIdsBeforeStoring;
        if (flushAndNotifyIntervalMillis <= 0) {
            throw new IllegalArgumentException("flushIntervalMillis must be greater than 0");
        }
        this.flushAndNotifyIntervalMillis = flushAndNotifyIntervalMillis;
        this.paranoidSyncMode = paranoidSyncMode;
        logger.info("Initialized storage backend: baseDirectory={}, hashIdsBeforeStoring={}, flushAndNotifyIntervalMillis={}, paranoidSyncMode={}", baseDirectory.getAbsolutePath(), hashIdsBeforeStoring, flushAndNotifyIntervalMillis, paranoidSyncMode);
    }

    private File validateBaseDirectory(File baseDir) {
        if (baseDir.exists() && !baseDir.isDirectory()) {
            throw new IllegalArgumentException(baseDir + " is not a directory");
        }
        try {
            Files.createDirectories(baseDir.toPath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create directory: " + baseDir.getAbsolutePath(), e);
        }
        return baseDir;
    }

    @Override
    public void prepareForWrite(String resourceId, boolean enableLineCounting) throws IOException {
        File file = resolveFileForId(resourceId);

        // Ensure datafile exists. This prevents concurrent cleanup from potentially deleting the containing directory.
        // This is probably over-engineered, but correct :D
        Path p = file.toPath();
        try {
            Files.createDirectories(p.getParent());
            Files.createFile(p);
        } catch (FileAlreadyExistsException ok) {
            // already present: done
        } catch (NoSuchFileException e) {
            // parent got pruned between the two calls: recreate and try once more
            Files.createDirectories(p.getParent());
            try {
                Files.createFile(p);
            } catch (FileAlreadyExistsException ok) {
                // created by someone else in the meantime: done
            }
        }

        if (enableLineCounting) {
            LinebreakIndexFile.createIfNeeded(file);
        }
        logger.debug("Prepared file {} for resource {}, indexed={}", file, resourceId, enableLineCounting);
    }

    @Override
    public LinebreakIndexFile getLinebreakIndex(String resourceId) throws IOException {
        return getLinebreakIndexFile(resourceId, false);
    }

    private LinebreakIndexFile getLinebreakIndexFile(String resourceId, boolean forWrite) throws IOException {
        File file = resolveFileForId(resourceId);
        if (forWrite) {
            return LinebreakIndexFile.openIfExistsForWrite(file, file.length());
        } else {
            return LinebreakIndexFile.openIfExists(file);
        }
    }

    @Override
    public void writeChunk(String resourceId, InputStream input, ThrowingConsumer<Long> fileSizeConsumer, ThrowingConsumer<Long> linebreakCountConsumer) throws IOException {
        // In the current implementation, uploads are one-shot, i.e. the file is uploaded in a single chunk. However,
        // it could also be a resuming/appending write, hence we account for that by "resuming" from the current file size (0 if non-existent)
        File file = resolveFileForId(resourceId);
        long startOffset = file.length();
        // NOTE: indexFile will be null if line-counting was not requested at registration time
        LinebreakIndexFile indexFile = getLinebreakIndexFile(resourceId, true);

        // This is something like a local method/function; we need to do it this way because we don't
        // want to explicitly create the streams in the try-with-resources below, as that would attempt to close them multiple times
        Function<OutputStream, OutputStream> actualOutputStreamDeterminingFunction = underlyingStream -> {
            if (indexFile != null) {
                // use provided stream, but wrapped with line break detection/update logic
                ThrowingConsumer<Long> linebreakPositionListener = linebreakPosition -> {
                    long newLinebreakCount = indexFile.addLinebreakPosition(linebreakPosition);
                    if (linebreakCountConsumer != null) {
                        linebreakCountConsumer.accept(newLinebreakCount);
                    }
                };
                return new LinebreakDetectingOutputStream(underlyingStream, linebreakPositionListener);
            } else {
                // no index file, directly use provided stream
                return underlyingStream;
            }
        };

        // This will auto-close the input and output streams, but we need to manually handle the indexFile because it
        // may be null.
        try (
                input;
                OutputStream checkpointingOut = new CheckpointingOutputStream(
                        actualOutputStreamDeterminingFunction.apply(FileChunk.getOutputStream(file, startOffset, paranoidSyncMode)),
                        flushAndNotifyIntervalMillis,
                        fileSizeConsumer
                )
        ) {
            long written = input.transferTo(checkpointingOut);
            logger.debug("Wrote {} bytes to file {}", written, file);
        } finally {
            if (indexFile != null) {
                indexFile.close();
            }
        }

        logger.debug("Wrote chunk to resource {} (current size: {} bytes)", resourceId, file.length());
    }

    @Override
    public InputStream openReadStream(String resourceId, long start, long end) throws IOException {
        // Micro-optimization for just-created, but not yet written-to, files
        if (start == 0 && end == 0) {
            return new ByteArrayInputStream(new byte[0]);
        }
        File file = resolveFileForId(resourceId);
        return FileChunk.getInputStream(file, start, end);
    }

    @Override
    public long getCurrentSize(String resourceId) throws IOException {
        File file = resolveFileForId(resourceId);
        return file.length();
    }

    /**
     * Handles a failed upload. This implementation currently does nothing,
     * as we decided to keep even failed data.
     *
     * @param resourceId the failed resource
     * @return {@code} true because data will still be present
     */
    @Override
    public boolean handleFailedUpload(String resourceId) {
        // Do nothing, indicate that data was kept
        return true;
    }

    /**
     * Resolves the file path for a given resource ID.
     */
    private File resolveFileForId(String id) {
        String path = hashIdsBeforeStoring ? hashId(id) : id;
        return buildFilePath(path, id);
    }

    private File buildFilePath(String hashedPath, String originalId) {
        String relPath = hashedPath.length() >= 4
                ? hashedPath.substring(0, 2) + File.separator + hashedPath.substring(2, 4)
                : "xx" + File.separator + "yy";
        return new File(baseDirectory, relPath + File.separator + originalId + ".bin");
    }

    private static String hashId(String id) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] hash = md.digest(id.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 not available", e);
        }
    }

    @Override
    public void delete(String resourceId) throws IOException {
        File baseFile = resolveFileForId(resourceId);
        Files.deleteIfExists(baseFile.toPath());
        Files.deleteIfExists(LinebreakIndexFile.getIndexFile(baseFile).toPath());
        // Opportunistic cleanup of empty parent directories
        deleteEmptyDirectories(baseFile.getParentFile());
    }

    // This method is safe even under extreme concurrency.
    // The creation of directories/files also uses atomic operations and handles potential edge cases introduced
    // by concurrent cleanup and creation, so no concurrency issues to expect.
    private void deleteEmptyDirectories(File leafDir) {
        Path baseDirPath = baseDirectory.toPath().toAbsolutePath().normalize();
        Path currentDirPath = leafDir.toPath().toAbsolutePath().normalize();

        while (currentDirPath != null && !currentDirPath.equals(baseDirPath)) {
            try {
                Files.delete(currentDirPath); // succeeds only if empty
            } catch (DirectoryNotEmptyException e) {
                break; // directory still in use: stop
            } catch (NoSuchFileException e) {
                // already gone (that was our intent anyway), keep walking up
            } catch (IOException e) {
                logger.debug("Aborting directory cleanup for {} because of unexpected exception: {}", currentDirPath, e.toString());
                break;
            }
            // move up to parent directory
            currentDirPath = currentDirPath.getParent();
        }
    }
}
