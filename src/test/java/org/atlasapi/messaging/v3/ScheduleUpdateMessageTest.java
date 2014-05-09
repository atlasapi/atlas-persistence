package org.atlasapi.messaging.v3;

import static org.junit.Assert.assertEquals;

import org.joda.time.DateTime;
import org.junit.Test;

import com.google.common.base.Charsets;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

public class ScheduleUpdateMessageTest {

    @Test
    public void testDeSerialization() throws Exception {
        
        ScheduleUpdateMessage msg
            = new ScheduleUpdateMessage("mid", Timestamp.of(1L), "bbc.co.uk", "hkqs", 
                new DateTime(0, DateTimeZones.UTC), new DateTime(86400, DateTimeZones.UTC));

        JacksonMessageSerializer<ScheduleUpdateMessage> serializer
            = JacksonMessageSerializer.forType(ScheduleUpdateMessage.class);

        byte[] serialized = serializer.serialize(msg);
        
        System.out.println(new String(serialized, Charsets.UTF_8));

        ScheduleUpdateMessage deserialized = serializer.deserialize(serialized);

        assertEquals(msg.getMessageId(), deserialized.getMessageId());
        assertEquals(msg.getTimestamp(), deserialized.getTimestamp());
        assertEquals(msg.getSource(), deserialized.getSource());
        assertEquals(msg.getChannel(), deserialized.getChannel());
        assertEquals(msg.getUpdateStart(), deserialized.getUpdateStart());
        assertEquals(msg.getUpdateEnd(), deserialized.getUpdateEnd());

    }

}
