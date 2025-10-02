package step.streaming.websocket.protocol.download;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import step.streaming.common.StreamingResourceStatus;

public class StatusChangedMessage extends DownloadServerMessage {
    public final StreamingResourceStatus resourceStatus;

    @JsonCreator
    public StatusChangedMessage(@JsonProperty("resourceStatus") StreamingResourceStatus resourceStatus) {
        this.resourceStatus = resourceStatus;
    }
}
