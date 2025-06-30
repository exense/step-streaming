package step.streaming.server;

import step.streaming.common.StreamingResourceReference;

/**
 * Interface for mapping between internal resource identifiers and external {@link StreamingResourceReference} representations.
 * <p>
 * This abstraction allows the server to translate between an internal, system-specific {@code resourceId}
 * (typically a string used internally for storage or lookup) and a {@link StreamingResourceReference},
 * which represents a more general, client-facing URI reference.
 * </p>
 */
public interface StreamingResourceReferenceMapper {

    /**
     * Converts a {@link StreamingResourceReference} into its corresponding internal resource ID.
     *
     * @param reference the external reference object (typically containing a URI); must not be {@code null}
     * @return the corresponding internal resource ID (e.g., UUID, file name, hash)
     * @throws IllegalArgumentException if the reference cannot be mapped
     */
    String referenceToResourceId(StreamingResourceReference reference);

    /**
     * Converts an internal resource ID into a {@link StreamingResourceReference}.
     *
     * @param resourceId the internal resource identifier (must not be {@code null})
     * @return the corresponding external {@link StreamingResourceReference}, typically wrapping a URI
     */
    StreamingResourceReference resourceIdToReference(String resourceId);
}
