package org.atlasapi.messaging.v3;

import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.base.Charsets;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

public class ScheduleUpdateMessageTest {

    private JacksonMessageSerializer<ScheduleUpdateMessage> serializer;

    @Before
    public void setUp() throws Exception {
        serializer = JacksonMessageSerializer.forType(ScheduleUpdateMessage.class);
    }

    @Test
    public void deserializeInvertsSerialize() throws Exception {
        ScheduleUpdateMessage msg
            = new ScheduleUpdateMessage("mid", Timestamp.of(1L), "bbc.co.uk", "hkqs", 
                new DateTime(0, DateTimeZones.UTC), new DateTime(86400, DateTimeZones.UTC));

        byte[] serialized = serializer.serialize(msg);
        
        ScheduleUpdateMessage deserialized = serializer.deserialize(serialized);

        assertEquals(msg.getMessageId(), deserialized.getMessageId());
        assertEquals(msg.getTimestamp(), deserialized.getTimestamp());
        assertEquals(msg.getSource(), deserialized.getSource());
        assertEquals(msg.getChannel(), deserialized.getChannel());
        assertEquals(msg.getUpdateStart(), deserialized.getUpdateStart());
        assertEquals(msg.getUpdateEnd(), deserialized.getUpdateEnd());
    }

    @Test
    public void deserializesObjectLongTimestamp() throws Exception {
        ScheduleUpdateMessage deserialized = serializer.deserialize(
                ("{"
                        + "\"@class\":\"org.atlasapi.messaging.v3.ScheduleUpdateMessage\","
                        + "\"messageId\":\"70281e3e-c71c-4bbb-810d-81f31592bf2f\","
                        + "\"timestamp\":{"
                        + "\"@class\":\"com.metabroadcast.common.time.Timestamp\","
                        + "\"millis\":[\"java.lang.Long\", 1467677805408]"
                        + "},"
                        + "\"source\":\"ebs.sport.bt.com\","
                        + "\"channel\":\"hn4x\","
                        + "\"updateStart\":["
                        + "\"org.joda.time.DateTime\","
                        + "1468260900000"
                        + "],"
                        + "\"updateEnd\":["
                        + "\"org.joda.time.DateTime\","
                        + "1468262700000"
                        + "]"
                        + "}").getBytes(Charsets.UTF_8));
        assertThat(deserialized.getChannel(), is("hn4x"));
    }

    @Test
    public void deserializesPrimitiveLongTimestamp() throws Exception {
        ScheduleUpdateMessage deserialized = serializer.deserialize(
                ("{"
                        + "\"@class\":\"org.atlasapi.messaging.v3.ScheduleUpdateMessage\","
                        + "\"messageId\":\"70281e3e-c71c-4bbb-810d-81f31592bf2f\","
                        + "\"timestamp\":{"
                        + "\"@class\":\"com.metabroadcast.common.time.Timestamp\","
                        + "\"millis\":1467677805408"
                        + "},"
                        + "\"source\":\"ebs.sport.bt.com\","
                        + "\"channel\":\"hn4x\","
                        + "\"updateStart\":["
                        + "\"org.joda.time.DateTime\","
                        + "1468260900000"
                        + "],"
                        + "\"updateEnd\":["
                        + "\"org.joda.time.DateTime\","
                        + "1468262700000"
                        + "]"
                        + "}").getBytes(Charsets.UTF_8));
        assertThat(deserialized.getChannel(), is("hn4x"));
    }
}
