package step.streaming.websocket.protocol.upload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import step.streaming.common.StreamingResourceReference;

public class ReadyForUploadMessage extends UploadServerMessage {
    public final StreamingResourceReference reference;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public ReadyForUploadMessage(@JsonProperty("reference") StreamingResourceReference reference) {
        this.reference = reference;
    }
}
