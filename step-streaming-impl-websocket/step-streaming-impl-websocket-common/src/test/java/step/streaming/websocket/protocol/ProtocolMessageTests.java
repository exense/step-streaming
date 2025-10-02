package step.streaming.websocket.protocol;

import org.junit.Test;
import step.streaming.websocket.protocol.download.RequestChunkMessage;

public class ProtocolMessageTests {

    @Test
    public void testThings() {
        // TODO: some simple serialization/deserialization/type tests
        System.err.println(new RequestChunkMessage(27, 45));
    }
}
