package step.streaming.websocket.protocol.upload;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import step.streaming.websocket.protocol.ProtocolMessage;

/** Protocol messages used for uploads.
 * Only for clarity and type-safety, an additional layer of (abstract) classes named
 * {@link UploadServerMessage} and {@link UploadClientMessage} exists to
 * indicate which end of the connection produces the messages.
 */
public class UploadProtocolMessage extends ProtocolMessage {
    public static final String UPLOAD_COMPLETED = "Upload completed";

    static {
        registerSubtypes(
                // unfortunately we cannot put the names as constants in the subclasses, as
                // that leads to a "potential JVM deadlock" warning
                new NamedType(RequestUploadStartMessage.class, "RequestUploadStart"),
                new NamedType(ReadyForUploadMessage.class, "ReadyForUpload"),
                new NamedType(UploadFinishedMessage.class, "UploadFinished")
        );
    }
}
