package org.atlasapi.persistence.messaging.mongo;

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.DateTimeZones;
import com.mongodb.BasicDBObjectBuilder;

import org.atlasapi.messaging.EntityUpdatedMessage;
import org.atlasapi.messaging.Message;
import org.joda.time.DateTime;

public class MongoMessageStoreTest {

    private final DatabasedMongo db = MongoTestHelper.anEmptyTestDatabase();
    private final MongoMessageStore store = new MongoMessageStore(db);
        
    @Before
    public void setUp() {
        db.collection("messages").ensureIndex(new BasicDBObjectBuilder()
            .append("timestamp", 1)
            .append("entitySource", 1)
        .get());
    }
    
    @Test 
    public void testStoreAndGet() {
        DateTime now = new DateTime(DateTimeZones.UTC);
        Message message1 = new EntityUpdatedMessage("1", now.getMillis(), "id1", "type", "pa");
        Message message2 = new EntityUpdatedMessage("2", now.plusHours(1).getMillis(), "id2", "type", "pa");
        Message message3 = new EntityUpdatedMessage("3", now.plusHours(2).getMillis(), "id3", "type", "pa");
        Message message4 = new EntityUpdatedMessage("4", now.plusHours(5).getMillis(), "id4", "type", "pa");
        
        store.add(message1);
        store.add(message2);
        store.add(message3);
        store.add(message4);
        
        Iterable<Message> messages = store.get(now.plusHours(1), now.plusHours(3), Optional.<String>absent());
        assertEquals(2, Iterables.size(messages));
        assertEquals(message2, Iterables.get(messages, 0));
        assertEquals(message3, Iterables.get(messages, 1));
    }
    
    @Test 
    public void testSourceFilter() {
        DateTime now = new DateTime(DateTimeZones.UTC);
        Message message1 = new EntityUpdatedMessage("1", now.getMillis(), "id1", "type", "pa");
        Message message2 = new EntityUpdatedMessage("2", now.plusHours(1).getMillis(), "id2", "type", "bbc");
        
        store.add(message1);
        store.add(message2);
        
        Iterable<Message> messages = store.get(
            now.minusHours(2), now.plusHours(3), 
            Optional.of("bbc"));
        assertEquals(1, Iterables.size(messages));
        assertEquals(message2, Iterables.get(messages, 0));
    }
}
