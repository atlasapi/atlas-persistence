package org.atlasapi.persistence.content.mongo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.DatabasedMongoClient;
import com.metabroadcast.common.text.NumberPadder;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.audit.PerHourAndDayMongoPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.content.PeopleListerListener;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

@RunWith( MockitoJUnitRunner.class )
public class MongoPersonStoreTest {

    private MongoClient mongo;
    private DatabasedMongo db;
    private DatabasedMongoClient mongoDatabase;
    private MongoPersonStore store;
    private MongoLookupEntryStore entryStore;
    private MongoContentWriter contentWriter;
    private ContentResolver contentResolver;
    private PersistenceAuditLog persistenceAuditLog;
    
    private final ServiceResolver serviceResolver = mock(ServiceResolver.class);
    private final PlayerResolver playerResolver = mock(PlayerResolver.class);
    
    private final String uri = "person1";
    
    @Before
    public void setUp() {
        mongo = MongoTestHelper.anEmptyMongo();
        db = new DatabasedMongo(mongo, "testing");
        mongoDatabase = new DatabasedMongoClient(mongo, "testing");
        persistenceAuditLog = new PerHourAndDayMongoPersistenceAuditLog(db);
        entryStore = new MongoLookupEntryStore(
                mongoDatabase,
                "peopleLookup",
                new NoLoggingPersistenceAuditLog(),
                ReadPreference.primary()
        );
        store = new MongoPersonStore(db, TransitiveLookupWriter.explicitTransitiveLookupWriter(entryStore), 
                entryStore, persistenceAuditLog);
        contentWriter = new MongoContentWriter(db, entryStore, persistenceAuditLog, 
                playerResolver, serviceResolver, new SystemClock());
        contentResolver = new LookupResolvingContentResolver(new MongoContentResolver(db, entryStore), entryStore);
    }
    
    @Test 
    public void testListingPeople() {
        int dummyPeopleToCreate = MongoPersonStore.MONGO_SCAN_BATCH_SIZE * 2;
        
        final List<Person> dummyPeople = Lists.newArrayList();
        for (int i = 0; i < dummyPeopleToCreate; i++) {
            Person person = new Person(NumberPadder.pad(i), "", Publisher.BBC);
            store.createOrUpdatePerson(person);
            dummyPeople.add(person);
        }
        
        final List<Person> peopleReturned = Lists.newArrayList();
        store.list(new PeopleListerListener() {
            
            @Override
            public void personListed(Person person) {
                peopleReturned.add(person);
            }
        });
        assertEquals(dummyPeopleToCreate, peopleReturned.size());
        assertEquals(dummyPeople, peopleReturned);
    }
    
    @Test
    public void shouldSetPersonAndAddItems() {
        Person person = new Person(uri, uri, Publisher.BBC);
        person.setId(1L);
        store.createOrUpdatePerson(person);
        
        List<String> items = Lists.newArrayList();
        for (long i=0; i<10; i++) {
            Item item = new Item("item"+i, "item"+i, Publisher.BBC);
            item.setId(i);
            items.add(item.getCanonicalUri());
            
            person.addContent(item.childRef());
            store.updatePersonItems(person);
        }
        
        for (long i=0; i<10; i++) {
            Item item = new Item("item"+i, "item"+i, Publisher.BBC);
            item.setId(i);
            person.addContent(item.childRef());
            store.updatePersonItems(person);
        }
        
        Person found = store.person(uri).get();
        assertNotNull(found);
        assertEquals(uri, found.getCanonicalUri());
        assertEquals(person.getId(), found.getId());
        assertEquals(person.getPublisher(), found.getPublisher());
        
        assertEquals(10, found.getContents().size());
        assertEquals(items, ImmutableList.copyOf(Iterables.transform(found.getContents(), ChildRef.TO_URI)));
        
        found = store.person(1L).get();
        
        assertEquals(uri, found.getCanonicalUri());
    }
    
    @Test
    public void testSetsIdOnCrewMemberInItem() {
        CrewMember crewMember = new CrewMember(uri, uri, Publisher.BBC);
        
        Item item = new Item("item", "item", Publisher.BBC);
        item.setId(2L);
        item.addPerson(crewMember);
        contentWriter.createOrUpdate(item);

        crewMemberId(item, null);
        
        Person person = crewMember.toPerson();
        person.setId(1L);
        person.setContents(ImmutableList.of(item.childRef()));
        store.createOrUpdatePerson(person);
        
        store.updatePersonItems(person);
        
        Long id = person.getId();
        crewMemberId(item, id);
        
    }

    private void crewMemberId(Item item, Long id) {
        ResolvedContent resolved = contentResolver.findByCanonicalUris(ImmutableList.of(item.getCanonicalUri()));
        Maybe<Identified> possibleContent = resolved.get(item.getCanonicalUri());
        Content content = (Content) possibleContent.requireValue();
        
        CrewMember resolvedCrew = Iterables.getOnlyElement(content.people());
        assertThat(resolvedCrew.getId(), is(id));
    }
    
}
