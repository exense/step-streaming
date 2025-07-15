package step.streaming.server;

import step.streaming.common.StreamingResourceReference;

import java.net.URI;
import java.util.function.Function;

public class URITemplateBasedReferenceProducer implements Function<String, StreamingResourceReference> {

    private URI baseUri; // may be null initially, must be set before apply() is actually called
    private final String pathWithParameterPlaceholder;
    private final String parameterName;

    public URITemplateBasedReferenceProducer(URI baseUri, String pathWithParameterPlaceholder, String parameterName) {
        this.baseUri = baseUri;
        this.pathWithParameterPlaceholder = pathWithParameterPlaceholder;
        this.parameterName = parameterName;

        if (!pathWithParameterPlaceholder.contains("{" + parameterName + "}")) {
            throw new IllegalArgumentException(String.format("path '%s' must contain '{%s}'", pathWithParameterPlaceholder, parameterName));
        }
    }

    @Override
    public StreamingResourceReference apply(String resourceId) {
        if (baseUri == null) {
            throw new IllegalStateException("Base URI must be set before calling apply()");
        }

        String resolvedPath = pathWithParameterPlaceholder.replace("{" + parameterName + "}", resourceId);
        URI fullUri = baseUri.resolve(resolvedPath);
        return new StreamingResourceReference(fullUri);
    }

    public void setBaseUri(URI uri) {
        this.baseUri = uri;
    }
}
