package step.streaming.websocket.protocol.download;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import step.streaming.websocket.protocol.ProtocolMessage;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Protocol messages used for downloads.
 * Only for clarity and type-safety, an additional layer of (abstract) classes named
 * {@link DownloadServerMessage} and {@link DownloadClientMessage} exists to
 * indicate which end of the connection produces the messages.
 */
public class DownloadProtocolMessage extends ProtocolMessage {
    private static final AtomicBoolean registered = new AtomicBoolean(false);

    static {
        initialize();
    }

    public static void initialize() {
        if (!registered.getAndSet(true)) {
            registerSubtypes(
                new NamedType(StatusChangedMessage.class, "StatusChanged"),
                new NamedType(RequestChunkMessage.class, "RequestChunk"),
                new NamedType(RequestLinesMessage.class, "RequestLines"),
                new NamedType(LinesMessage.class, "Lines")
            );
        }
    }
}
