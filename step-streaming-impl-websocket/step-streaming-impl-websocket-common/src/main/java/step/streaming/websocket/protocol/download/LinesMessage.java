package step.streaming.websocket.protocol.download;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import step.streaming.common.StreamingResourceStatus;

import java.util.List;

public class LinesMessage extends DownloadServerMessage {
    public final List<String> lines;

    @JsonCreator
    public LinesMessage(@JsonProperty("lines") List<String> lines) {
        this.lines = lines;
    }
}
