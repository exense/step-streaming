package step.streaming.server;

import step.streaming.common.StreamingResourceReference;

import java.net.URI;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DefaultStreamingResourceReferenceMapper implements StreamingResourceReferenceMapper {
    // baseUri may be left empty initially if unknown at construction time, but must be set before actually performing operations.
    private URI baseUri;
    private final String placeholder; // e.g. "{id}"
    private final String pathTemplate; // e.g. "/some/path/{id}"
    private final Pattern extractionPattern; // compiled regex for resourceId extraction

    public DefaultStreamingResourceReferenceMapper(URI baseUri, String pathTemplate, String placeholder) {
        if (baseUri != null) setBaseUri(baseUri);
        if (!Objects.requireNonNull(placeholder).matches("\\{[a-zA-Z_][a-zA-Z0-9_]*}")) {
            throw new IllegalArgumentException("Invalid placeholder: " + placeholder + ": must start with '{' and end with '}', and consist of alphanumeric characters");
        }
        this.placeholder = placeholder;
        this.pathTemplate = Objects.requireNonNull(pathTemplate);
        if (!pathTemplate.contains(placeholder)) {
            throw new IllegalArgumentException("Template must contain " + placeholder);
        }
        String regex = Pattern.quote(pathTemplate).replace(Pattern.quote(placeholder), "([^/]+)");
        this.extractionPattern = Pattern.compile("^" + regex + "$");
    }

    public void setBaseUri(URI baseUri) {
        this.baseUri = Objects.requireNonNull(baseUri);
    }

    @Override
    public StreamingResourceReference resourceIdToReference(String resourceId) {
        checkInitialized();
        String path = pathTemplate.replace(placeholder, encodeSegment(resourceId));
        URI uri = baseUri.resolve(path);
        return new StreamingResourceReference(uri);
    }

    @Override
    public String referenceToResourceId(StreamingResourceReference reference) {
        checkInitialized();

        URI full = reference.getUri();
        if (!full.toString().startsWith(baseUri.toString())) {
            throw new IllegalArgumentException("URI " + full + " does not match base URI " + baseUri);
        }

        String path = full.getPath();
        String basePath = baseUri.getPath();
        String relativePath = path.startsWith(basePath) ? path.substring(basePath.length()) : path;

        Matcher matcher = extractionPattern.matcher(relativePath);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Path " + relativePath + " does not match expected pattern " + pathTemplate);
        }

        String encodedId = matcher.group(1);
        return decodeSegment(encodedId);
    }

    private void checkInitialized() {
        if (baseUri == null || pathTemplate == null || extractionPattern == null) {
            throw new IllegalStateException("baseUri and pathTemplate must be set before use");
        }
    }

    private String encodeSegment(String segment) {
        try {
            return new URI(null, null, "/" + segment, null).getPath().substring(1);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to encode URI segment: " + segment, e);
        }
    }

    private String decodeSegment(String encoded) {
        try {
            return URI.create("/" + encoded).getPath().substring(1);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to decode URI segment: " + encoded, e);
        }
    }
}
