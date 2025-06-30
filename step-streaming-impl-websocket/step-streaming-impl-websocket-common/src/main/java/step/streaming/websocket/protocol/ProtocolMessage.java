package step.streaming.websocket.protocol;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A message encoded as Json.
 * For easy interoperability with JS-based browser implementations, we use short type names
 * using the "@" property.
 * This requires to explicitly specify the subtype mappings, which is done using the
 * static method `registerSubtypes` in the base implementing subclasses.
 * Doing it in code, rather than as annotations, has the advantage of being modular and
 * extensible -- annotations would have to specify all known leaf types at the top level (i.e., this) class.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "@")
public class ProtocolMessage {
    private static final AtomicReference<ObjectMapper> MAPPER = new AtomicReference<>(new ObjectMapper());
    private static final AtomicReference<ObjectReader> READER = new AtomicReference<>(MAPPER.get().readerFor(ProtocolMessage.class));
    private static final AtomicReference<ObjectWriter> WRITER = new AtomicReference<>(MAPPER.get().writerFor(ProtocolMessage.class));

    private static final Logger logger = LoggerFactory.getLogger(ProtocolMessage.class);

    protected static void registerSubtypes(NamedType... types) {
        if (logger.isDebugEnabled()) {
            logger.debug("Registering subtypes: {}", Arrays.toString(types));
        }
        MAPPER.set(MAPPER.get().copy());
        MAPPER.get().registerSubtypes(types);
        READER.set(MAPPER.get().readerFor(ProtocolMessage.class));
        WRITER.set(MAPPER.get().writerFor(ProtocolMessage.class));
    }

    /**
     * Deserializes a JSON string into a {@link ProtocolMessage} instance.
     * For convenience, the return type is polymorphic, i.e., it may be
     * specified by the caller as any subclass of ProtocolMessage, therefore
     * this method is implicitly performing a cast to the requested type.
     *
     * @param string the JSON string to parse
     * @param <T>    the specific type of ProtocolMessage to return.
     * @return the deserialized message instance
     * @throws RuntimeException if deserialization fails
     */
    public static <T extends ProtocolMessage> T fromString(String string) {
        try {
            return READER.get().readValue(string);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Serializes this ProtocolMessage instance to its JSON string representation.
     *
     * @return the JSON string
     * @throws RuntimeException if serialization fails
     */
    @Override
    public final String toString() {
        try {
            return WRITER.get().writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
