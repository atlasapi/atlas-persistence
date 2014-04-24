package org.atlasapi.messaging.v3;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;
import org.atlasapi.messaging.worker.v3.AbstractMessageConfiguration;
import org.atlasapi.messaging.worker.v3.AdjacentRefConfiguration;
import org.atlasapi.messaging.worker.v3.ContentEquivalenceAssertionMessageConfiguration;
import org.atlasapi.serialization.json.JsonFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Objects;
import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.queue.MessageDeserializationException;
import com.metabroadcast.common.queue.MessageSerializationException;
import com.metabroadcast.common.queue.MessageSerializer;
import com.metabroadcast.common.time.Timestamp;

public class JacksonMessageSerializer<M extends Message> implements MessageSerializer<M> {

    public static final <M extends Message> JacksonMessageSerializer<M> forType(Class<? extends M> cls) {
        return new JacksonMessageSerializer<M>(cls);
    }
    
    public static class MessagingModule extends SimpleModule {

        public MessagingModule() {
            super("Messaging Module", new com.fasterxml.jackson.core.Version(0, 0, 1, null, null, null));
        }

        @Override
        public void setupModule(SetupContext context) {
            super.setupModule(context);
            context.setMixInAnnotations(EntityUpdatedMessage.class, AbstractMessageConfiguration.class);
            context.setMixInAnnotations(ContentEquivalenceAssertionMessage.class, 
                ContentEquivalenceAssertionMessageConfiguration.class);
            context.setMixInAnnotations(AdjacentRef.class, 
                    AdjacentRefConfiguration.class);
            context.setMixInAnnotations(Timestamp.class, TimestampConfiguration.class);
        }
    }
    
    private final ObjectMapper mapper = JsonFactory.makeJsonMapper()
            .registerModule(new MessagingModule());
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
