package step.streaming.common;

import java.util.Objects;

/**
 * Holds metadata about a streaming resource, such as the original filename
 * and its MIME type.
 * <p>
 * This metadata is typically supplied by clients when uploading a file, and
 * may be used by servers or downloaders to interpret or store the file appropriately.
 */
public class StreamingResourceMetadata {

    private String filename;
    private String mimeType;
    private boolean supportsLineAccess;

    /**
     * Regular expression used to validate MIME types of the form "type/subtype",
     * such as "text/plain" or "application/json".
     */
    private static final String MIME_TYPE_PATTERN = "^[a-zA-Z0-9!#$&^_.+-]+/[a-zA-Z0-9!#$&^_.+-]+$";

    /**
     * Default constructor for deserialization or frameworks.
     */
    public StreamingResourceMetadata() {
    }

    /**
     * Constructs a {@code StreamingResourceMetadata} with the given filename and MIME type.
     * Validates the MIME type format upon construction. Line access support will automatically
     * be determined by the MIME type given (though it may later be overridden)
     *
     * @param filename the name of the file (must not be {@code null})
     * @param mimeType the MIME type of the file (must match {@link #MIME_TYPE_PATTERN})
     * @throws NullPointerException     if filename is {@code null}
     * @throws IllegalArgumentException if MIME type is {@code null} or invalid, or filename is empty
     */
    public StreamingResourceMetadata(String filename, String mimeType) {
        setFilename(filename);
        setMimeType(mimeType);
        setSupportsLineAccess(mimeType.toLowerCase().startsWith("text/"));
    }

    /**
     * Returns the file name associated with this resource.
     *
     * @return the filename (never {@code null})
     */
    public String getFilename() {
        return filename;
    }

    /**
     * Sets the filename.
     *
     * @param filename the filename to set (must not be {@code null})
     * @throws NullPointerException if filename is {@code null}
     * @throws IllegalArgumentException if filename is empty
     */
    public void setFilename(String filename) {
        this.filename = Objects.requireNonNull(filename, "Filename must not be null");
        if (filename.isEmpty()) {
            throw new IllegalArgumentException("Filename must not be empty");
        }
    }

    /**
     * Returns the MIME type of the resource.
     *
     * @return the MIME type
     */
    public String getMimeType() {
        return mimeType;
    }

    /**
     * Sets the MIME type for the resource.
     *
     * @param mimeType the MIME type to set (must match {@link #MIME_TYPE_PATTERN})
     * @throws IllegalArgumentException if the MIME type is {@code null} or invalid
     */
    public void setMimeType(String mimeType) {
        if (mimeType == null || !mimeType.matches(MIME_TYPE_PATTERN)) {
            throw new IllegalArgumentException("Invalid MIME type format: " + mimeType);
        }
        this.mimeType = mimeType;
    }

    public boolean getSupportsLineAccess() {
        return supportsLineAccess;
    }

    public void setSupportsLineAccess(boolean supportsLineAccess) {
        this.supportsLineAccess = supportsLineAccess;
    }

    /**
     * Common MIME type constants.
     */
    @SuppressWarnings("unused")
    public static class CommonMimeTypes {
        /** MIME type for plain text. */
        public static final String TEXT_PLAIN = "text/plain";

        /** MIME type for HTML text. */
        public static final String TEXT_HTML = "text/html";

        /** MIME type for JSON documents. */
        public static final String APPLICATION_JSON = "application/json";

        /** Generic MIME type for binary data. */
        public static final String APPLICATION_OCTET_STREAM = "application/octet-stream";

        private CommonMimeTypes() {} //utility class
    }

    @Override
    public String toString() {
        return "StreamingResourceMetadata{" +
                "filename='" + filename + '\'' +
                ", mimeType='" + mimeType + '\'' +
                ", supportsLineAccess=" + supportsLineAccess +
                '}';
    }
}
