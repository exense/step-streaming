package step.streaming.websocket.server;

import jakarta.websocket.Session;

public interface WebsocketServerEndpointSessionsHandler {
    void register(Session session);

    void unregister(Session session);

    void shutdown();
}
