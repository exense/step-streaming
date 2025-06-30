package step.streaming.websocket.test;

import org.junit.Test;

public class SmokeTest {
    @Test
    public void smokeTest() throws Exception {
        var server = new TestingWebsocketServer().start();
        Thread.sleep(1000);
        server.stop();
    }

}
