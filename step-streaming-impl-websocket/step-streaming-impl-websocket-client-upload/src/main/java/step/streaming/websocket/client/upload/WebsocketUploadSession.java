package step.streaming.websocket.client.upload;

import step.streaming.client.upload.impl.BasicStreamingUploadSession;
import step.streaming.common.StreamingResourceMetadata;
import step.streaming.data.EndOfInputSignal;

/**
 * Represents a streaming upload using a WebSocket transport.
 * <p>
 */
public class WebsocketUploadSession extends BasicStreamingUploadSession {
    // basically the entire implementation is now in BasicStreamingUploadSession
    public WebsocketUploadSession(StreamingResourceMetadata metadata, EndOfInputSignal endOfInputSignal) {
        super(metadata, endOfInputSignal);
    }

    @Override
    public String toString() {
        return String.format("WebsocketUploadSession{metadata=%s, currentStatus=%s}", metadata, getCurrentStatus());
    }
}
