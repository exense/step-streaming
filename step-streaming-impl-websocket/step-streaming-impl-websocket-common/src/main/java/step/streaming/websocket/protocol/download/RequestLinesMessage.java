package step.streaming.websocket.protocol.download;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RequestLinesMessage extends DownloadClientMessage {
    public final long startingLineIndex;
    public final long linesCount;

    @JsonCreator
    public RequestLinesMessage(@JsonProperty("startingLineIndex") long startingLineIndex, @JsonProperty("linesCount") long linesCount) {
        this.startingLineIndex = startingLineIndex;
        this.linesCount = linesCount;
    }
}
