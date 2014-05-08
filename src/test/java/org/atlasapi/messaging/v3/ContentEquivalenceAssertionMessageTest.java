package org.atlasapi.messaging.v3;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.time.Timestamp;


public class ContentEquivalenceAssertionMessageTest {

    @Test
    public void testDeSerialization() throws Exception {
        
        Set<String> srcs = Sets.newHashSet(Publisher.BBC.key(), Publisher.ITV.key());
        List<AdjacentRef> adjacent = Lists.newArrayList(
            new AdjacentRef("cyp", "episode", Publisher.PA.key()),
            new AdjacentRef("dms", "item", Publisher.ITV.key())
        );
        
        ContentEquivalenceAssertionMessage msg = new ContentEquivalenceAssertionMessage(
                "1", Timestamp.of(1), "cf2", "episode", "bbc.co.uk", adjacent, srcs);

        JacksonMessageSerializer<ContentEquivalenceAssertionMessage> serializer
            = JacksonMessageSerializer.forType(ContentEquivalenceAssertionMessage.class);
        
        byte[] serialized = serializer.serialize(msg);
        
        ContentEquivalenceAssertionMessage deserialized 
            = serializer.deserialize(serialized);
        
        assertEquals(msg.getMessageId(), deserialized.getMessageId());
        assertEquals(msg.getTimestamp(), deserialized.getTimestamp());
        assertEquals(msg.getEntityId(), deserialized.getEntityId());
        assertEquals(msg.getEntityType(), deserialized.getEntityType());
        assertEquals(msg.getEntitySource(), deserialized.getEntitySource());
        assertEquals(msg.getAdjacent(), deserialized.getAdjacent());
        assertEquals(msg.getSources(), deserialized.getSources());
        
    }

}
