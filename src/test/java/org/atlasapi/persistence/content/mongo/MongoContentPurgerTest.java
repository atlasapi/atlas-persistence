package org.atlasapi.persistence.content.mongo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.applications.client.model.internal.ApplicationConfiguration;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.DatabasedMongoClient;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.testing.ComplexItemTestDataBuilder;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.audit.PerHourAndDayMongoPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.DefaultEquivalentContentResolver;
import org.atlasapi.persistence.content.EquivalenceWritingContentWriter;
import org.atlasapi.persistence.content.LookupResolvingContentResolver;
import org.atlasapi.persistence.lookup.TransitiveLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith( MockitoJUnitRunner.class )
public class MongoContentPurgerTest {

    private static final String MB_ITEM_URI = "http://metabroadcast.com/programmes/item";
    private static final String BBC_ITEM_URI = "http://www.bbc.co.uk/programmes/item";
    private MongoClient mongoClient;
    private DatabasedMongo db;
    private DatabasedMongoClient mongoDatabase;
    private EquivalenceWritingContentWriter contentWriter;
    private MongoContentPurger mongoContentPurger;
    private DefaultEquivalentContentResolver contentResolver;
    private MongoLookupEntryStore entryStore;
    private PersistenceAuditLog persistenceAuditLog;
    
    private final ServiceResolver serviceResolver = mock(ServiceResolver.class);
    private final PlayerResolver playerResolver = mock(PlayerResolver.class);
    private Application application = mock(Application.class);
 
    @Before
    public void setUp() {
        mongoClient = MongoTestHelper.anEmptyMongo();
        db = new DatabasedMongo(mongoClient, "testing");
        mongoDatabase = new DatabasedMongoClient(mongoClient, "testing");
        persistenceAuditLog = new PerHourAndDayMongoPersistenceAuditLog(db);
        DBCollection lookupCollection = db.collection("lookup");
        entryStore = new MongoLookupEntryStore(
                mongoDatabase.collection("lookup", DBObject.class),
                mongoDatabase,
                new NoLoggingPersistenceAuditLog(),
                ReadPreference.primary()
        );
        contentWriter = new EquivalenceWritingContentWriter(
                new MongoContentWriter(db, entryStore, persistenceAuditLog, 
                                       playerResolver, serviceResolver, 
                                       new SystemClock()),
                TransitiveLookupWriter.explicitTransitiveLookupWriter(entryStore));
        contentResolver = new DefaultEquivalentContentResolver(new MongoContentResolver(db, entryStore), entryStore);
        
        
        mongoContentPurger = 
                new MongoContentPurger(new MongoContentLister(db, 
                        new MongoContentResolver(db, entryStore)), 
                                       new LookupResolvingContentResolver(new MongoContentResolver(db, entryStore), entryStore),
                                       contentWriter, new MongoContentTables(db), lookupCollection, 
                                       TransitiveLookupWriter.explicitTransitiveLookupWriter(entryStore),
                                       TransitiveLookupWriter.generatedTransitiveLookupWriter(entryStore));

        when(application.getConfiguration()).thenReturn(
                ApplicationConfiguration.builder()
                        .withPrecedence(ImmutableList.of(Publisher.BBC, Publisher.METABROADCAST))
                        .withEnabledWriteSources(ImmutableList.of())
                        .build()
        );
    }
    
    @Test
    public void testRemovesContentAndUpdatesEquivalences() {
        Item item1 = testItem(Publisher.BBC, BBC_ITEM_URI);
        Item item2 = testItem(Publisher.METABROADCAST, MB_ITEM_URI);
        
        item2.setEquivalentTo(ImmutableSet.of(LookupRef.from(item1)));
        contentWriter.createOrUpdate(item1);
        contentWriter.createOrUpdate(item2);
        
        LookupEntry lookup = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableSet.of(BBC_ITEM_URI)));
        assertEquals(2, lookup.explicitEquivalents().getLookupRefs().size());
        
        mongoContentPurger.purge(Publisher.METABROADCAST, ImmutableSet.of(Publisher.BBC));
        
        lookup = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableSet.of(BBC_ITEM_URI)));
        assertEquals(1, lookup.explicitEquivalents().getLookupRefs().size());
        
        assertEquals(0, contentResolver.resolveUris(ImmutableSet.of(BBC_ITEM_URI), application, ImmutableSet.<Annotation>of(), false).get(MB_ITEM_URI).size());
        
        Content queried = Iterables.getOnlyElement(contentResolver.resolveUris(ImmutableSet.of(BBC_ITEM_URI), application, ImmutableSet.<Annotation>of(), false).get(BBC_ITEM_URI));
        assertEquals(new Alias(MongoContentPurger.ATLAS_EQUIVALENCE_ALIAS, MB_ITEM_URI), Iterables.getOnlyElement(queried.getAliases()));
       
    }

    @Test
    public void testRestoresEquivs() {
        Item bbcItem = testItem(Publisher.BBC, BBC_ITEM_URI);
        Item mbItem = testItem(Publisher.METABROADCAST, MB_ITEM_URI);
        
        mbItem.setEquivalentTo(ImmutableSet.of(LookupRef.from(bbcItem)));
        contentWriter.createOrUpdate(bbcItem);
        contentWriter.createOrUpdate(mbItem);
        
        mongoContentPurger.purge(Publisher.METABROADCAST, ImmutableSet.of(Publisher.BBC));

        Item newMbItem = testItem(Publisher.METABROADCAST, MB_ITEM_URI);
        contentWriter.createOrUpdate(newMbItem);
        
        mongoContentPurger.restoreEquivalences(Publisher.BBC);
        
        Set<Content> content = contentResolver.resolveUris(ImmutableSet.of(BBC_ITEM_URI), application, ImmutableSet.<Annotation>of(), false).get(BBC_ITEM_URI);
        assertEquals(2, content.size());
        
        assertEquals(0, Iterables.getFirst(content, null).getAliases().size());
        
    }

    private Item testItem(Publisher publisher, String uri) {
        Item item = ComplexItemTestDataBuilder.complexItem().withUri(uri).build();
        item.setPublisher(publisher);
        return item;
    }
}
