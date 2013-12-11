package org.atlasapi.messaging.worker.v3;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;

import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.serialization.json.JsonFactory;
import org.junit.Test;


public class AbstractWorkerTest {

    @Test
    public void testOnMessage() throws Exception {
        
        final EntityUpdatedMessage msg = new EntityUpdatedMessage("1", 1L, "id", "type", "src");
        final AtomicBoolean processed = new AtomicBoolean(false);
        
        AbstractWorker w = new AbstractWorker() {
            @Override
            public void process(EntityUpdatedMessage message) {
                assertEquals(msg.getMessageId(), message.getMessageId());
                assertEquals(msg.getTimestamp(), message.getTimestamp());
                assertEquals(msg.getEntityId(), message.getEntityId());
                assertEquals(msg.getEntityType(), message.getEntityType());
                assertEquals(msg.getEntitySource(), message.getEntitySource());
                processed.set(true);
            }
        };
        
        w.onMessage(JsonFactory.makeJsonMapper().writeValueAsString(msg));
        
        assertTrue(processed.get());
    }

}
