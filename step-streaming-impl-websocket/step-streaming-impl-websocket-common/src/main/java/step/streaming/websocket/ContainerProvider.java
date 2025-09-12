//package step.streaming.websocket;
//
//import java.net.http.HttpClient;
//
//public class ContainerProvider {
//    private ContainerProvider() {}
//
//    public static void getWebSocketContainer() {
//// Build once at app startup
//        QueuedThreadPool exec = new QueuedThreadPool(16); // limit client worker threads
//        exec.setName("ws-client");
//        exec.setReservedThreads(0);
//
//        HttpClient http = new HttpClient();
//        http.setExecutor(exec);
//        http.start();
//
//        WebSocketClient wsClient = new WebSocketClient(http);
//        wsClient.setStopAtShutdown(true);   // extra safety
//        wsClient.start();
//
//        JakartaWebSocketClientContainer container =
//                new JakartaWebSocketClientContainer(wsClient);
//        container.start();
//
//// Reuse this 'container' for all connectToServer(...) calls
//
//    }
//}
