package step.streaming.websocket.protocol.upload;

import com.fasterxml.jackson.databind.jsontype.NamedType;
import step.streaming.websocket.protocol.ProtocolMessage;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Protocol messages used for uploads.
 * Only for clarity and type-safety, an additional layer of (abstract) classes named
 * {@link UploadServerMessage} and {@link UploadClientMessage} exists to
 * indicate which end of the connection produces the messages.
 */
public class UploadProtocolMessage extends ProtocolMessage {
    public static final String CLOSEREASON_PHRASE_UPLOAD_COMPLETED = "Upload completed";

    private static final AtomicBoolean registered = new AtomicBoolean(false);

    static {
        initialize();
    }

    public static void initialize() {
        if (!registered.getAndSet(true)) {
            registerSubtypes(
                // unfortunately we cannot put the names as constants in the subclasses, as
                // that leads to a "potential JVM deadlock" warning
                new NamedType(StartUploadMessage.class, "StartUpload"),
                new NamedType(ReadyForUploadMessage.class, "ReadyForUpload"),
                new NamedType(FinishUploadMessage.class, "FinishUpload"),
                new NamedType(UploadAcknowledgedMessage.class, "UploadAcknowledged")
            );
        }
    }
}
