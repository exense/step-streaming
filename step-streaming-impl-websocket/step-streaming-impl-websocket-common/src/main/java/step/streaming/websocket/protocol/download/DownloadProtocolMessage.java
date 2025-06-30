package step.streaming.websocket.protocol.download;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import step.streaming.websocket.protocol.ProtocolMessage;

/** Protocol messages used for downloads.
 * Only for clarity and type-safety, an additional layer of (abstract) classes named
 * {@link DownloadServerMessage} and {@link DownloadClientMessage} exists to
 * indicate which end of the connection produces the messages.
 */
public class DownloadProtocolMessage extends ProtocolMessage {
    static {
        registerSubtypes(
                new NamedType(StatusChangedMessage.class, "StatusChanged"),
                new NamedType(RequestChunkMessage.class, "RequestChunk")
        );
    }
}
