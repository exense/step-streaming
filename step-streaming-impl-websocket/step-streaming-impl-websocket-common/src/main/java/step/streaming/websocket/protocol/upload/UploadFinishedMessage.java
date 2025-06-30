package step.streaming.websocket.protocol.upload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class UploadFinishedMessage extends UploadServerMessage {
    public final long finalSize;
    public final String checksum;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public UploadFinishedMessage(@JsonProperty("finalSize") long finalSize, @JsonProperty("checksum") String checksum) {
        this.finalSize = finalSize;
        this.checksum = checksum;
    }
}
