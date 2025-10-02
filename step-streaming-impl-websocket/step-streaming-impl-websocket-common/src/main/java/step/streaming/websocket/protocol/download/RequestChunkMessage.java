package step.streaming.websocket.protocol.download;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RequestChunkMessage extends DownloadClientMessage {
    public final long startOffset;
    public final long endOffset;

    @JsonCreator
    public RequestChunkMessage(@JsonProperty("startOffset") long startOffset, @JsonProperty("endOffset") long endOffset) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }
}
