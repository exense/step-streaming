package step.streaming.common;

import java.net.URI;
import java.util.Objects;

/**
 * A reference to a streaming resource, represented as a URI.
 * <p>
 */
public class StreamingResourceReference {

    private URI uri;
    private String resourceId;

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
    public StreamingResourceReference(URI uri, String resourceId) {
        this.uri = Objects.requireNonNull(uri, "URI must not be null");
        this.resourceId = resourceId; // might be null in client-only scenarios
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

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    @Override
    public String toString() {
        return Objects.toString(uri);
    }
}
