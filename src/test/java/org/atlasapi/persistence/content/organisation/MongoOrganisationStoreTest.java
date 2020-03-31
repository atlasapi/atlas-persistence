package org.atlasapi.persistence.content.organisation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.DatabasedMongoClient;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.audit.PerHourAndDayMongoPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class MongoOrganisationStoreTest {

        private MongoClient mongoClient;
        private DatabasedMongo db;
        private DatabasedMongoClient mongoDatabase;
        private MongoOrganisationStore store;
        private MongoLookupEntryStore entryStore;
        private PersistenceAuditLog persistenceAuditLog;
        
        private final String uri = "organisation1";
        
        @Before
        public void setUp() {
            mongoClient = MongoTestHelper.anEmptyMongo();
            db = new DatabasedMongo(mongoClient, "testing");
            mongoDatabase = new DatabasedMongoClient(mongoClient, "testing");
            persistenceAuditLog = new PerHourAndDayMongoPersistenceAuditLog(db);
            entryStore = new MongoLookupEntryStore(
                    mongoDatabase,
                    "peopleLookup",
                    new NoLoggingPersistenceAuditLog(),
                    ReadPreference.primary()
            );
            store = new MongoOrganisationStore(db, TransitiveLookupWriter.explicitTransitiveLookupWriter(entryStore), 
                    entryStore, persistenceAuditLog);
        }
        
        @Test
        public void shouldSetOrganisationAndAddItems() {
            Organisation organisation = new Organisation(ImmutableList.of(new Person("person1", "", Publisher.BBC)));
            organisation.setPublisher(Publisher.BBC);
            organisation.setCanonicalUri(uri);
            organisation.setId(1L);
            organisation.setAlternativeTitles(ImmutableSet.of("title1"));
            store.createOrUpdateOrganisation(organisation);
            
            List<String> items = Lists.newArrayList();
            for (long i=0; i<10; i++) {
                Item item = new Item("item"+i, "item"+i, Publisher.BBC);
                item.setId(i);
                items.add(item.getCanonicalUri());
                
                organisation.addContent(item.childRef());
                store.updateOrganisationItems(organisation);
            }
            
            for (long i=0; i<10; i++) {
                Item item = new Item("item"+i, "item"+i, Publisher.BBC);
                item.setId(i);
                organisation.addContent(item.childRef());
                store.updateOrganisationItems(organisation);
            }
            
            Organisation found = store.organisation(uri).get();
            assertNotNull(found);
            assertEquals(uri, found.getCanonicalUri());
            assertEquals(organisation.getId(), found.getId());
            assertEquals(organisation.getPublisher(), found.getPublisher());
            assertEquals("title1",Iterables.getOnlyElement(found.getAlternativeTitles()));
            
            assertEquals(10, found.getContents().size());
            assertEquals(items, ImmutableList.copyOf(Iterables.transform(found.getContents(), ChildRef.TO_URI)));
            
            found = store.organisation(1L).get();
            
            assertEquals(uri, found.getCanonicalUri());
        }
}