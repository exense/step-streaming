package step.streaming.websocket;

import jakarta.websocket.*;

import java.io.IOException;
import java.nio.channels.ClosedChannelException;

/**
 * Websocket endpoint class that is resilient to failures which may occur on Websocket session closure
 * in certain infrastructure/deployment situations.
 * <p>
 * This has been observed to happen on Kubernetes instances (only), and is likely caused by some combination
 * of intercepting/NATing software (nginx/kubernetes), and the jetty implementation (IOW: it might be a bug in Jetty).
 * In short, Websockets use a two-way "handshake" to close sessions, where the initiator (the endpoint which
 * wants to close the session) sends the close frame, and expects a response from the other side before actually
 * terminating the TCP connection. In certain situations (such as with K8s), the TCP connection is terminated earlier
 * than Jetty is expecting it, causing a ClosedChannelException instead of the close confirmation.
 * <p>
 * This class detects this specific situation and signals a normal termination instead.
 * <p>
 * See <a href="https://github.com/jetty/jetty.project/issues/13346#issuecomment-3069318279">Github bug report</a>
 * describing the situation. This might be fixed in future versions of Jetty, in which case this class would become
 * obsolete.
 * <p>
 * The workaround affects the logic of the {@code onClose} and {@code onError} methods; to prevent accidental
 * incorrect use, these methods have been declared as final. Subclasses should implement their respective logic
 * in the {@code onSessionClose/onSessionError} methods.
 * <p>
 * <b>IMPORTANT NOTE: Subclasses MUST use the {@code closeSession} method instead of directly
 * invoking {@code session.close()}.</b>
 */

public abstract class HalfCloseCompatibleEndpoint extends Endpoint {

    private volatile CloseReason closeReason;
    private volatile boolean closed = false;

    /**
     * Closes the Websocket socket session in a manner that is compatible with the half-close workaround.
     * <p>
     * <b>Implementations MUST use this method instead of invoking {@code session.close()} directly.</b>
     *
     * @param session     session to close
     * @param closeReason close reason
     * @throws IOException on error
     * @see Session#close(CloseReason) 
     */
    public final void closeSession(Session session, CloseReason closeReason) throws IOException {
        this.closeReason = closeReason;
        session.close(closeReason);
    }

    /**
     * final {@code onError} implementation with half-session-close workaround. Implementations should override
     * {@link HalfCloseCompatibleEndpoint#onSessionError(Session, Throwable)} instead.
     *
     * @param session   the session in use when the error occurs.
     * @param throwable the throwable representing the problem.
     * @see HalfCloseCompatibleEndpoint#onSessionError(Session, Throwable)
     */
    @Override
    public final void onError(Session session, Throwable throwable) {
        if (throwable instanceof ClosedChannelException && closeReason != null) {
            // Here is the actual workaround. Swallow this particular exception and perform the normal close handling instead
            onClose(session, closeReason);
        } else {
            onSessionError(session, throwable);
        }
    }

    /**
     * final {@code onClose} implementation with half-session-close workaround. Implementations should override
     * {@link HalfCloseCompatibleEndpoint#onSessionClose(Session, CloseReason)} instead.
     *
     * @param session     the session about to be closed.
     * @param closeReason the reason the session was closed.
     * @see HalfCloseCompatibleEndpoint#onSessionClose(Session, CloseReason)
     */
    @Override
    public final void onClose(Session session, CloseReason closeReason) {
        /* Only inform implementations once; in case of the workaround being needed,
           this method will be called twice: once by the actual workaround (instead of
           throwing an exception), and once by the framework signaling "abnormal"
           closure -- in which case we swallow that second call.
        */
        if (!closed) {
            closed = true;
            onSessionClose(session, closeReason);
        }
    }

    // Javadoc copied from original onError method doc.

    /**
     * Developers may implement this method when the web socket session creates some kind of error that is not modeled
     * in the web socket protocol. This may for example be a notification that an incoming message is too big to handle,
     * or that the incoming message could not be encoded.
     *
     * <p>
     * There are a number of categories of exception that this method is (currently) defined to handle:
     * <ul>
     * <li>connection problems, for example, a socket failure that occurs before the web socket connection can be
     * formally closed. These are modeled as {@link SessionException}s</li>
     * <li>runtime errors thrown by developer created message handlers calls.</li>
     * <li>conversion errors encoding incoming messages before any message handler has been called. These are modeled as
     * {@link DecodeException}s</li>
     * </ul>
     *
     * @param session the session in use when the error occurs.
     * @param thr     the throwable representing the problem.
     */
    public void onSessionError(Session session, Throwable thr) {

    }

    // Javadoc copied from the original onClose

    /**
     * This method is called immediately prior to the session with the remote peer being closed. It is called whether
     * the session is being closed because the remote peer initiated a close and sent a close frame, or whether the
     * local websocket container or this endpoint requests to close the session. The developer may take this last
     * opportunity to retrieve session attributes such as the ID, or any application data it holds before it becomes
     * unavailable after the completion of the method. Developers should not attempt to modify the session from within
     * this method, or send new messages from this call as the underlying connection will not be able to send them at
     * this stage.
     *
     * @param session     the session about to be closed.
     * @param closeReason the reason the session was closed.
     */
    public void onSessionClose(Session session, CloseReason closeReason) {

    }
}
