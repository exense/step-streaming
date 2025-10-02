package step.streaming.websocket.test;

import jakarta.websocket.server.ServerEndpointConfig;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.websocket.jakarta.server.config.JakartaWebSocketServletContainerInitializer;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class TestingWebsocketServer {
    private Server server;
    private ServerConnector connector;

    private final List<Class<?>> annotatedServerEndpointClasses = new ArrayList<>();
    private final List<ServerEndpointConfig> endpointConfigs = new ArrayList<>();

    public TestingWebsocketServer withEndpoints(Class<?>... annotatedServerEndpointClasses) {
        this.annotatedServerEndpointClasses.addAll(List.of(annotatedServerEndpointClasses));
        return this;
    }

    public TestingWebsocketServer withEndpointConfigs(ServerEndpointConfig... endpointConfigs) {
        this.endpointConfigs.addAll(List.of(endpointConfigs));
        return this;
    }

    public TestingWebsocketServer() {

    }

    public TestingWebsocketServer start() throws Exception {
        return start(0); // OS assigns a free port
    }

    public TestingWebsocketServer start(int port) throws Exception {
        server = new Server();

        connector = new ServerConnector(server);
        connector.setPort(port);
        server.addConnector(connector);

        ServletContextHandler servletContextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        servletContextHandler.setContextPath("/");
        server.setHandler(servletContextHandler);

        JakartaWebSocketServletContainerInitializer.configure(servletContextHandler, (servletContext, container) -> {
            for (Class<?> endpointClass : annotatedServerEndpointClasses) {
                container.addEndpoint(endpointClass);
            }
            for (ServerEndpointConfig endpointConfig : endpointConfigs) {
                container.addEndpoint(endpointConfig);
            }
        });
        server.start();
        return this;
    }

    public URI getURI() {
        int port = connector.getLocalPort();
        return URI.create("ws://localhost:" + port + "/");
    }

    public URI getURI(String endpoint, String... params) {
        if (params.length > 0) {
            endpoint = endpoint.replaceFirst("\\{[^}]+}", params[0]);
        }
        return getURI().resolve(endpoint);
    }

    public void stop() throws Exception {
        if (server != null) {
            server.stop();
            server.join(); // wait for shutdown
        }
    }
}
