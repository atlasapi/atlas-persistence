package org.atlasapi.persistence.messaging.mongo;

import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import org.atlasapi.messaging.EntityUpdatedMessage;
import org.atlasapi.messaging.Message;
import org.joda.time.DateTime;

public class MongoMessageStoreTest {

    private DatabasedMongo db;
    private MongoMessageStore store;
        
    @Before
    public void setUp() {
        db = MongoTestHelper.anEmptyTestDatabase();
        store = new MongoMessageStore(db);
    }
    
    @Test 
    public void testStoreAndGet() {
        DateTime now = new DateTime();
        Message message1 = new EntityUpdatedMessage("1", now.getMillis(), null, null, null);
        Message message2 = new EntityUpdatedMessage("2", now.plusHours(1).getMillis(), null, null, null);
        Message message3 = new EntityUpdatedMessage("3", now.plusHours(2).getMillis(), null, null, null);
        Message message4 = new EntityUpdatedMessage("4", now.plusHours(5).getMillis(), null, null, null);
        
        store.add(message1);
        store.add(message2);
        store.add(message3);
        store.add(message4);
        
        Iterable<Message> messages = store.get(now.plusHours(1), now.plusHours(3));
        assertEquals(2, Iterables.size(messages));
        assertEquals(message2, Iterables.get(messages, 0));
        assertEquals(message3, Iterables.get(messages, 1));
    }
}
