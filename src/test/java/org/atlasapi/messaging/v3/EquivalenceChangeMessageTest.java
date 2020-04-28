package org.atlasapi.messaging.v3;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.time.Timestamp;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class EquivalenceChangeMessageTest {

    @Test
    public void testDeSerialization() throws Exception {

        EquivalenceChangeMessage msg = new EquivalenceChangeMessage(
                "id",
                Timestamp.of(0L),
                1L,
                ImmutableSet.of(2L, 3L),
                ImmutableSet.of(4L, 5L),
                ImmutableSet.of(6L, 7L),
                ImmutableSet.of(Publisher.TESTING_MBST.key(), Publisher.BBC.key())

        );
        JacksonMessageSerializer<EquivalenceChangeMessage> serializer =
                JacksonMessageSerializer.forType(EquivalenceChangeMessage.class);

        byte[] serialized = serializer.serialize(msg);

        EquivalenceChangeMessage deserialized
                = serializer.deserialize(serialized);

        assertEquals(msg.getMessageId(), deserialized.getMessageId());
        assertEquals(msg.getTimestamp(), deserialized.getTimestamp());
        assertEquals(msg.getSubjectId(), deserialized.getSubjectId());
        assertEquals(msg.getOutgoingIdsAdded(), deserialized.getOutgoingIdsAdded());
        assertEquals(msg.getOutgoingIdsRemoved(), deserialized.getOutgoingIdsRemoved());
        assertEquals(msg.getOutgoingIdsUnchanged(), deserialized.getOutgoingIdsUnchanged());
        assertEquals(msg.getSources(), deserialized.getSources());

        assertEquals(msg.getOutgoingIdsChanged(), deserialized.getOutgoingIdsChanged());
        assertEquals(msg.getOutgoingIds(), deserialized.getOutgoingIds());

    }

}
