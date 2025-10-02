package step.streaming.websocket.protocol.upload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import step.streaming.common.StreamingResourceMetadata;

public class StartUploadMessage extends UploadClientMessage {
    public final StreamingResourceMetadata metadata;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public StartUploadMessage(@JsonProperty("metadata") StreamingResourceMetadata metadata) {
        this.metadata = metadata;
    }
}
