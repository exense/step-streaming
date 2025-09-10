package step.streaming.websocket.server;

import jakarta.websocket.CloseReason;
import jakarta.websocket.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import step.streaming.websocket.CloseReasonUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A class for managing server-side endpoint sessions.
 * Since the implementation is expected to set session timeouts to infinite (0) generally,
 * this handler class will send periodic one-byte keep-alive messages on all active sessions.
 * The ping interval is 25 seconds (just in case a session is registered that does not explicitly
 * disable the timeout, which is 30 seconds by default).
 * <p>
 * This is to ensure that sessions are actually still alive (thus also keeping them alive on
 * potential middle layers like nginx etc.), and to detect abnormal transfer situations, closing
 * the sessions abnormally if that is the case.
 * <p>
 * This class will also terminate any currently active sessions on shutdown.
 */
public class DefaultWebsocketServerEndpointSessionsHandler implements WebsocketServerEndpointSessionsHandler {
    private static final Logger logger = LoggerFactory.getLogger(DefaultWebsocketServerEndpointSessionsHandler.class);

    private final ScheduledExecutorService scheduler;
    private final Set<Session> sessions = ConcurrentHashMap.newKeySet();
    private final ByteBuffer pingPayload = ByteBuffer.wrap(new byte[]{42}).asReadOnlyBuffer();
    private static final long PING_INTERVAL_SECONDS = 25;

    public DefaultWebsocketServerEndpointSessionsHandler() {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, this.getClass().getSimpleName());
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(this::sendPings, PING_INTERVAL_SECONDS, PING_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void register(Session session) {
        session.setMaxIdleTimeout(0);
        sessions.add(Objects.requireNonNull(session));
    }

    @Override
    public void unregister(Session session) {
        if (session != null) {
            sessions.remove(session);
        }
    }

    private void sendPings() {
        for (Session session : sessions) {
            if (session.isOpen()) {
                try {
                    if (logger.isTraceEnabled()) {
                        logger.trace("Sending ping to session {}", session.getId());
                    }
                    session.getAsyncRemote().sendPing(pingPayload);
                } catch (IOException e) {
                    logger.warn("Failed to send ping to session {}", session.getId(), e);
                }
            } else {
                unregister(session);
            }
        }
    }

    @Override
    public void shutdown() {
        scheduler.shutdownNow();
        for (Session session : Set.copyOf(sessions)) {
            sessions.remove(session);
            if (session.isOpen()) {
                try {
                    session.close(CloseReasonUtil.makeSafeCloseReason(CloseReason.CloseCodes.GOING_AWAY, "Server shutdown"));
                } catch (IOException e) {
                    logger.warn("Failed to close session {}", session.getId(), e);
                }
            }
        }
    }
}
