package step.streaming.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.data.CheckpointingOutputStream;
import step.streaming.data.FileChunk;
import step.streaming.data.util.ThrowingConsumer;
import step.streaming.server.data.LinebreakDetectingOutputStream;
import step.streaming.server.data.LinebreakIndexFile;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.function.Consumer;
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
 * I had initially enabled it (without the flag), only to wonder why aeverything was extremely slow.
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
    }

    private File validateBaseDirectory(File baseDir) {
        if (baseDir.exists() && !baseDir.isDirectory()) {
            throw new IllegalArgumentException(baseDir + " is not a directory");
        }
        if (!baseDir.exists() && !baseDir.mkdirs()) {
            throw new RuntimeException("Failed to create directory: " + baseDir);
        }
        return baseDir;
    }

    @Override
    public void prepareForWrite(String resourceId, boolean enableLineCounting) throws IOException {
        File file = resolveFileForId(resourceId, true);
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
        File file = resolveFileForId(resourceId, false);
        if (forWrite) {
            return LinebreakIndexFile.openIfExistsForWrite(file, file.length());
        } else {
            return LinebreakIndexFile.openIfExists(file);
        }
    }

    @Override
    public void writeChunk(String resourceId, InputStream input, Consumer<Long> fileSizeConsumer, Consumer<Long> linebreakCountConsumer) throws IOException {
        // In the current implementation, uploads are one-shot, i.e. the file is uploaded in a single chunk. However,
        // it could also be a resuming/appending write, hence we account for that by "resuming" from the current file size (0 if non-existent)
        File file = resolveFileForId(resourceId, false);
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
        } catch (IOException e) {
            logger.error("Failed to write chunk for resource {}", resourceId, e);
            // Implementation note: Some uploads (e.g. Websockets) DO NOT necessarily throw an Exception on the InputStream!
            // Instead, the transfer will seemingly complete correctly, but the session is then closed abnormally.
            throw e;
        } finally {
            if (indexFile != null) {
                indexFile.close();
            }
        }

        logger.debug("Wrote chunk to resource {} (current size: {} bytes)", resourceId, file.length());
    }

    @Override
    public InputStream openReadStream(String resourceId, long start, long end) throws IOException {
        // special case for no-yet-present resources
        if (start == 0 && end == 0) {
            return new ByteArrayInputStream(new byte[0]);
        }
        File file = resolveFileForId(resourceId, false);
        return FileChunk.getInputStream(file, start, end);
    }

    @Override
    public long getCurrentSize(String resourceId) throws IOException {
        File file = resolveFileForId(resourceId, false);
        return file.length();
    }

    /**
     * Handles a failed upload. This implementation currently does nothing,
     * as we decided to keep even failed data.
     * @param resourceId the failed resource
     */
    public void handleFailedUpload(String resourceId) {
    }

    /**
     * Resolves the file path for a given resource ID.
     */
    private File resolveFileForId(String id, boolean createDirectories) throws IOException {
        String path = hashIdsBeforeStoring ? hashId(id) : id;
        File file = buildFilePath(path, id);

        if (createDirectories) {
            File parent = file.getParentFile();
            if (!parent.exists() && !parent.mkdirs()) {
                throw new IOException("Failed to create directory: " + parent);
            }
        }
        return file;
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
}
