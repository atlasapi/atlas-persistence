package org.atlasapi.messaging.v3;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Objects;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageDeserializationException;
import com.metabroadcast.common.queue.MessageSerializationException;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;
import org.atlasapi.messaging.worker.v3.AdjacentRefConfiguration;
import org.atlasapi.messaging.worker.v3.ContentEquivalenceAssertionMessageConfiguration;
import org.atlasapi.messaging.worker.v3.EntityUpdatedMessageConfiguration;
import org.atlasapi.messaging.worker.v3.EquivalenceChangeMessageConfiguration;
import org.atlasapi.messaging.worker.v3.ScheduleUpdateMessageConfiguration;
import org.atlasapi.serialization.json.JsonFactory;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

public class JacksonMessageSerializer<M extends Message> implements MessageSerializer<M> {


    public static final <M extends Message> JacksonMessageSerializer<M> forType(Class<? extends M> cls) {
        return new JacksonMessageSerializer<>(cls);
    }

    /* This exists because
        1. we made a boo-boo
        2. there is now a message serialization kerfaffle

      The weird Timestamp class can be instantiated by both java.lang.Long and a long primitive,
      which really confuses Jackson (and confused a dev here at some point). So we made a boo-boo
      and broke serialization compatibility, and had

        - the old format, primitive {"@class": "... Timestamp", "millis": 42}
        - the new format, object {"@class": "... Timestamp", ["java.lang.Long", 42]}

      It's impossible to cope with both of those via just mixins, so we had to make a custom
      deserializer like this.
     */
    public static class TimestampDeserializer extends JsonDeserializer<Timestamp> {

        @Override
        public Timestamp deserialize(
                JsonParser jp,
                DeserializationContext ctxt
        ) throws IOException {
            JsonToken token = jp.nextValue();

            if (!"millis".equals(jp.getCurrentName())) {
                throw new JsonParseException("Expected millis field", jp.getCurrentLocation());
            }

            Timestamp result;

            if (token == JsonToken.START_ARRAY) {
                JsonToken javaLangLongToken = jp.nextToken();
                if (javaLangLongToken != JsonToken.VALUE_STRING) {
                    throw new JsonParseException(
                            "Expected string token with class name",
                            jp.getCurrentLocation()
                    );
                } else if (!"java.lang.Long".equals(jp.getText())) {
                    throw new JsonParseException(
                            "Expected class name to be java.lang.Long",
                            jp.getCurrentLocation()
                    );
                }

                JsonToken numberToken = jp.nextToken();
                if (numberToken != JsonToken.VALUE_NUMBER_INT) {
                    throw new JsonParseException(
                            "Expected number token",
                            jp.getCurrentLocation()
                    );
                }

                Long timestamp = jp.getLongValue();

                JsonToken arrayCloseToken = jp.nextToken();
                if (arrayCloseToken != JsonToken.END_ARRAY) {
                    throw new JsonParseException(
                            "Expected array end token",
                            jp.getCurrentLocation()
                    );
                }

                result = Timestamp.of(timestamp);
            } else if (token == JsonToken.VALUE_NUMBER_INT) {
                result = Timestamp.of(jp.getValueAsLong());
            } else {
                throw new JsonParseException(
                        "Could not parse millis field",
                        jp.getCurrentLocation()
                );
            }

            token = jp.nextToken();
            if (token != JsonToken.END_OBJECT) {
                throw new JsonParseException(
                        "Expected end of object",
                        jp.getCurrentLocation()
                );
            }

            return result;
        }
    }

    public static class MessagingModule extends SimpleModule {

        public MessagingModule() {
            super("Messaging Module", new com.fasterxml.jackson.core.Version(0, 0, 1, null, null, null));
        }

        @Override
        public void setupModule(SetupContext context) {
            super.setupModule(context);
            context.setMixInAnnotations(
                    EntityUpdatedMessage.class,
                    EntityUpdatedMessageConfiguration.class
            );
            context.setMixInAnnotations(
                    ContentEquivalenceAssertionMessage.class,
                    ContentEquivalenceAssertionMessageConfiguration.class
            );
            context.setMixInAnnotations(AdjacentRef.class, AdjacentRefConfiguration.class);

            context.setMixInAnnotations(EquivalenceChangeMessage.class, EquivalenceChangeMessageConfiguration.class);

            SimpleDeserializers deserializers = new SimpleDeserializers();
            deserializers.addDeserializer(Timestamp.class, new TimestampDeserializer());
            context.addDeserializers(deserializers);

            context.setMixInAnnotations(
                    ScheduleUpdateMessage.class,
                    ScheduleUpdateMessageConfiguration.class
            );

        }
    }
    
    private final ObjectMapper mapper = JsonFactory.makeJsonMapper()
            .registerModule(new MessagingModule())
            .registerModule(new GuavaModule())
            .registerModule(new JodaModule());

    private final Class<? extends M> cls;
    
    public JacksonMessageSerializer(Class<? extends M> cls) {
        this.cls = checkNotNull(cls);
    }
    
    @Override
    public byte[] serialize(M message) throws MessageSerializationException {
        try {
            return mapper.writeValueAsBytes(message);
        } catch (IOException ioe) {
            throw new MessageSerializationException(message.toString(), ioe);
        }
    }

    @Override
    public M deserialize(byte[] serialized) throws MessageDeserializationException {
        try {
            return mapper.readValue(serialized, cls);
        } catch (IOException e) {
            throw new MessageDeserializationException(e);
        }
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
            .addValue(cls.getSimpleName())
            .toString();
    }

}
