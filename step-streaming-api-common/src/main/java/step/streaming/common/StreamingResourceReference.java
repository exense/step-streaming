package step.streaming.common;

import java.net.URI;
import java.util.Objects;

/**
 * A reference to a streaming resource, represented as a URI.
 */
public class StreamingResourceReference {

    private URI uri;

    /**
     * Default constructor for serialization/deserialization frameworks.
     */
    public StreamingResourceReference() {
    }

    /**
     * Constructs a new {@code StreamingResourceReference} with the given URI.
     *
     * @param uri the URI pointing to the resource (must not be null)
     * @throws NullPointerException if uri is null
     */
    public StreamingResourceReference(URI uri) {
        this.uri = Objects.requireNonNull(uri, "URI must not be null");
    }

    /**
     * Returns the URI representing the location of the resource.
     *
     * @return the URI of the resource
     */
    public URI getUri() {
        return uri;
    }

    /**
     * Sets the URI for this resource reference.
     *
     * @param uri the URI to set (must not be null)
     * @throws NullPointerException if uri is null
     */
    public void setUri(URI uri) {
        this.uri = Objects.requireNonNull(uri, "URI must not be null");
    }

    @Override
    public String toString() {
        return Objects.toString(uri);
    }
}
