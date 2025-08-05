package step.streaming.websocket.protocol.upload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import step.streaming.common.StreamingResourceMetadata;

public class FinishUploadMessage extends UploadClientMessage {
    public final String checksum;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public FinishUploadMessage(@JsonProperty("checksum") String checksum) {
        this.checksum = checksum;
    }
}
