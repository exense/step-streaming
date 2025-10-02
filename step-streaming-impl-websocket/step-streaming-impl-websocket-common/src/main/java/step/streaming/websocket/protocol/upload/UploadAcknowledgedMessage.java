package step.streaming.websocket.protocol.upload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class UploadAcknowledgedMessage extends UploadServerMessage {
    public final long size;
    public final Long numberOflines;
    public final String checksum;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public UploadAcknowledgedMessage(@JsonProperty("size") long size, @JsonProperty("numberOfLines") Long numberOflines, @JsonProperty("checksum") String checksum) {
        this.size = size;
        this.numberOflines = numberOflines;
        this.checksum = checksum;
    }
}
