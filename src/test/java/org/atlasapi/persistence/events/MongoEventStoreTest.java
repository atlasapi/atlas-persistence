package org.atlasapi.persistence.events;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.event.EventStore;
import org.atlasapi.persistence.event.MongoEventStore;

import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.junit.Test;

import static org.atlasapi.persistence.events.EventTranslatorTest.createEvent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;


public class MongoEventStoreTest {
    
    private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    private final EventStore store = new MongoEventStore(mongo);

    @Test
    public void testNewEventStorage() {
        Event event = createEvent(ImmutableList.<Topic>of());
        long id = 1234L;
        event.setId(id);
        
        store.createOrUpdate(event);
        Optional<Event> fetched = store.fetch(id);
        
        assertEquals(event, fetched.get());
    }

    @Test
    public void writingEventReturnsWrittenEvent() throws Exception {
        Event event = createEvent(ImmutableList.<Topic>of());
        long id = 1234L;
        event.setId(id);

        Event writtenEvent = store.createOrUpdate(event);
        Optional<Event> fetched = store.fetch(id);

        assertEquals(writtenEvent, fetched.get());
    }

    @Test
    public void testFetchOfAbsentEvent() {
        
        Optional<Event> fetched = store.fetch(1234L);
        
        assertFalse(fetched.isPresent());
    }
    
    @Test
    public void testFetchByEventGroup() {
        Topic eventGroup = EventTranslatorTest.createTopic("uri", "value");
        eventGroup.setId(349830L);
        Event event = createEvent(ImmutableList.of(eventGroup));
        event.setId(1234L);
        
        Topic notMatching = EventTranslatorTest.createTopic("uri", "value");
        notMatching.setId(43092L);
        Event event2 = createEvent(ImmutableList.of(notMatching));
        event2.setId(34985L);
        
        store.createOrUpdate(event);
        store.createOrUpdate(event2);
        
        Iterable<Event> fetched = store.fetch(Optional.of(eventGroup), Optional.<DateTime>absent());
        
        assertEquals(event, Iterables.getOnlyElement(fetched));
    }
    
    @Test
    public void testFetchWithFromTimestamp() {
        DateTime now = DateTime.now();
        
        Event pastEvent = createEvent(ImmutableList.<Topic>of());
        pastEvent.setEndTime(now.minusDays(5));
        pastEvent.setId(1234L);
        
        Event futureEvent = createEvent(ImmutableList.<Topic>of());
        futureEvent.setEndTime(now.plusDays(1));
        futureEvent.setId(34985L);
        
        store.createOrUpdate(pastEvent);
        store.createOrUpdate(futureEvent);
        
        Iterable<Event> fetched = store.fetch(Optional.<Topic>absent(), Optional.of(now));
        
        assertEquals(futureEvent, Iterables.getOnlyElement(fetched));
    }
    
    @Test
    public void testFetchWithFromTimestampAndEventGroup() {
        DateTime now = DateTime.now();
        Topic eventGroup = EventTranslatorTest.createTopic("matching", "value");
        Topic notMatching = EventTranslatorTest.createTopic("not matching", "value");
        eventGroup.setId(349830L);
        notMatching.setId(43092L);
        
        Event matchingFuture = createEvent(ImmutableList.of(eventGroup));
        matchingFuture.setEndTime(now.plusDays(1));
        matchingFuture.setId(34985L);
        
        Event nonMatchingFuture = createEvent(ImmutableList.of(notMatching));
        nonMatchingFuture.setEndTime(now.plusDays(1));
        nonMatchingFuture.setId(59839l);
        
        Event matchingPastEvent = createEvent(ImmutableList.of(eventGroup));
        matchingPastEvent.setEndTime(now.minusDays(5));
        matchingPastEvent.setId(1234L);
        
        store.createOrUpdate(matchingPastEvent);
        store.createOrUpdate(nonMatchingFuture);
        store.createOrUpdate(matchingFuture);
        
        Iterable<Event> fetched = store.fetch(Optional.of(eventGroup), Optional.of(now));
        
        assertEquals(matchingFuture, Iterables.getOnlyElement(fetched));
    }
    
    @Test
    public void testFetchAllEvents() {
        Event event = createEvent(ImmutableList.<Topic>of());
        event.setId(1234L);
        
        Event event2 = createEvent(ImmutableList.<Topic>of());
        event2.setId(3948l);
        
        store.createOrUpdate(event);
        store.createOrUpdate(event2);

        Iterable<Event> allEvents = store.fetch(Optional.<Topic>absent(), Optional.<DateTime>absent());
        
        assertEquals(ImmutableSet.of(event, event2), ImmutableSet.copyOf(allEvents));
    }
    
    @Test
    public void testFetchesEventsOrderedByStartDate() {
        DateTime now = DateTime.now();
        Event newer = createEvent(ImmutableList.<Topic>of());
        newer.setStartTime(now.plusDays(2));
        newer.setId(1234L);
        
        Event older = createEvent(ImmutableList.<Topic>of());
        older.setStartTime(now);
        older.setId(3948l);
        
        store.createOrUpdate(newer);
        store.createOrUpdate(older);

        Iterable<Event> allEvents = store.fetch(Optional.<Topic>absent(), Optional.<DateTime>absent());
        
        assertEquals(older, Iterables.getFirst(allEvents, null));
        assertEquals(newer, Iterables.get(allEvents, 1));
    }
}
