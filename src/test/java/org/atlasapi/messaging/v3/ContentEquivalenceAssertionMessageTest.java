package org.atlasapi.messaging.v3;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;
import org.atlasapi.messaging.worker.v3.AbstractWorker.MessagingModule;
import org.atlasapi.serialization.json.JsonFactory;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


public class ContentEquivalenceAssertionMessageTest {

    @Test
    public void testDeSerialization() throws Exception {
        
        Set<Publisher> srcs = Sets.newHashSet(Publisher.BBC, Publisher.ITV);
        List<AdjacentRef> adjacent = Lists.newArrayList(
            new AdjacentRef(2L, "episode", Publisher.PA),
            new AdjacentRef(3L, "item", Publisher.ITV)
        );
        
        ContentEquivalenceAssertionMessage msg = new ContentEquivalenceAssertionMessage(
                "1", 1L, "sid", "episode", "bbc.co.uk", adjacent, srcs);
     
        ObjectMapper mapper = JsonFactory.makeJsonMapper()
            .registerModule(new MessagingModule());
        
        String serialized = mapper.writeValueAsString(msg);
        
        System.out.println(serialized);
        
        ContentEquivalenceAssertionMessage deserialized 
            = mapper.readValue(serialized, ContentEquivalenceAssertionMessage.class);
        
        assertEquals(msg.getMessageId(), deserialized.getMessageId());
        assertEquals(msg.getTimestamp(), deserialized.getTimestamp());
        assertEquals(msg.getEntityId(), deserialized.getEntityId());
        assertEquals(msg.getEntityType(), deserialized.getEntityType());
        assertEquals(msg.getEntitySource(), deserialized.getEntitySource());
        assertEquals(msg.getAdjacent(), deserialized.getAdjacent());
        assertEquals(msg.getSources(), deserialized.getSources());
        
    }

}
