package org.atlasapi.messaging.worker.v3;

import java.io.IOException;

import org.atlasapi.messaging.v3.BeginReplayMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;
import org.atlasapi.messaging.v3.EndReplayMessage;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.Message;
import org.atlasapi.messaging.v3.ReplayMessage;
import org.atlasapi.serialization.json.JsonFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;


/**
 * Base {@link org.atlasapi.persistence.messaging.worker.Worker} class providing
 * {@link org.atlasapi.persistence.messaging.Message} unmarshaling and
 * dispatching.
 */
public abstract class AbstractWorker implements Worker {
    
    public static class MessagingModule extends SimpleModule {

        public MessagingModule() {
            super("Messaging Module", new com.fasterxml.jackson.core.Version(0, 0, 1, null, null, null));
        }

        @Override
        public void setupModule(SetupContext context) {
            super.setupModule(context);
            context.setMixInAnnotations(EntityUpdatedMessage.class, AbstractMessageConfiguration.class);
            context.setMixInAnnotations(BeginReplayMessage.class, AbstractMessageConfiguration.class);
            context.setMixInAnnotations(ReplayMessage.class, ReplayMessageConfiguration.class);
            context.setMixInAnnotations(EndReplayMessage.class, AbstractMessageConfiguration.class);
            context.setMixInAnnotations(ContentEquivalenceAssertionMessage.class, 
                ContentEquivalenceAssertionMessageConfiguration.class);
            context.setMixInAnnotations(AdjacentRef.class, 
                    AdjacentRefConfiguration.class);
        }
    }

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper()
            .registerModule(new MessagingModule());

    public void onMessage(String message) {
        try {
            Message event = mapper.readValue(message, Message.class);
            event.dispatchTo(this);
        } catch (IOException ex) {
            throw new RuntimeException(ex.getMessage(), ex);
        }
    }

    @Override
    public void process(EntityUpdatedMessage message) {
    }

    @Override
    public void process(BeginReplayMessage message) {
    }

    @Override
    public void process(EndReplayMessage message) {
    }

    @Override
    public void process(ReplayMessage message) {
    }
    
    @Override
    public void process(ContentEquivalenceAssertionMessage message) {
    }
    
}
