package org.atlasapi.messaging.v3;

import static org.junit.Assert.*;

import org.junit.Test;

import com.metabroadcast.common.time.Timestamp;


public class EntityUpdatedMessageTest {

    @Test
    public void testDeSerialization() throws Exception {
        
        EntityUpdatedMessage msg
            = new EntityUpdatedMessage("id", Timestamp.of(1L), "cbbh", "item", "bbc.co.uk");
        
        JacksonMessageSerializer<EntityUpdatedMessage> serializer = 
            JacksonMessageSerializer.forType(EntityUpdatedMessage.class);
        
        byte[] serialized = serializer.serialize(msg);
        
        EntityUpdatedMessage deserialized 
            = serializer.deserialize(serialized);
        
        assertEquals(msg.getMessageId(), deserialized.getMessageId());
        assertEquals(msg.getTimestamp(), deserialized.getTimestamp());
        assertEquals(msg.getEntityId(), deserialized.getEntityId());
        assertEquals(msg.getEntityType(), deserialized.getEntityType());
        assertEquals(msg.getEntitySource(), deserialized.getEntitySource());
    }

}
