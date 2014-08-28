package org.atlasapi.persistence.events;

import static org.atlasapi.persistence.events.EventTranslatorTest.createEvent;
import static org.junit.Assert.*;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.event.EventStore;
import org.atlasapi.persistence.event.MongoEventStore;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;


public class MongoEventStoreTest {
    
    private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    private final EventStore store = new MongoEventStore(mongo);

    @Test
    public void testNewEventStorage() {
        Event event = createEvent(ImmutableList.<Topic>of());
        long id = 1234l;
        event.setId(id);
        
        store.createOrUpdate(event);
        Optional<Event> fetched = store.fetch(id);
        
        assertEquals(event, fetched.get());
    }
    
    @Test
    public void testFetchOfAbsentEvent() {
        
        Optional<Event> fetched = store.fetch(1234l);
        
        assertFalse(fetched.isPresent());
    }
    
    @Test
    public void testFetchByEventGroup() {
        Topic eventGroup = EventTranslatorTest.createTopic("uri", "value");
        eventGroup.setId(349830l);
        Event event = createEvent(ImmutableList.of(eventGroup));
        event.setId(1234l);
        
        Topic notMatching = EventTranslatorTest.createTopic("uri", "value");
        notMatching.setId(43092l);
        Event event2 = createEvent(ImmutableList.of(notMatching));
        event2.setId(34985l);
        
        store.createOrUpdate(event);
        store.createOrUpdate(event2);
        
        Iterable<Event> fetched = store.fetchByEventGroup(eventGroup);
        
        assertEquals(event, Iterables.getOnlyElement(fetched));
    }
    
    @Test
    public void testFetchAllEvents() {
        Event event = createEvent(ImmutableList.<Topic>of());
        event.setId(1234l);
        
        Event event2 = createEvent(ImmutableList.<Topic>of());
        event2.setId(3948l);
        
        store.createOrUpdate(event);
        store.createOrUpdate(event2);

        Iterable<Event> allEvents = store.fetchAll();
        
        assertEquals(ImmutableSet.of(event, event2), ImmutableSet.copyOf(allEvents));
    }
}
