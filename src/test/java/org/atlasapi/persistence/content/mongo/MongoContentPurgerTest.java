package org.atlasapi.persistence.content.mongo;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.Set;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.application.v3.SourceStatus;
import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.testing.ComplexItemTestDataBuilder;
import org.atlasapi.output.Annotation;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.SystemClock;

@RunWith( MockitoJUnitRunner.class )
public class MongoContentPurgerTest {

    private static final String MB_ITEM_URI = "http://metabroadcast.com/programmes/item";
    private static final String BBC_ITEM_URI = "http://www.bbc.co.uk/programmes/item";
    private DatabasedMongo db;
    private EquivalenceWritingContentWriter contentWriter;
    private MongoContentPurger mongoContentPurger;
    private DefaultEquivalentContentResolver contentResolver;
    private MongoLookupEntryStore entryStore;
    private PersistenceAuditLog persistenceAuditLog;
    
    private final ServiceResolver serviceResolver = mock(ServiceResolver.class);
    private final PlayerResolver playerResolver = mock(PlayerResolver.class);
 
    @Before
    public void setUp() {
        db = MongoTestHelper.anEmptyTestDatabase();
        persistenceAuditLog = new PerHourAndDayMongoPersistenceAuditLog(db);
        entryStore = new MongoLookupEntryStore(db.collection("lookup"));
        contentWriter = new EquivalenceWritingContentWriter(
                new MongoContentWriter(db, entryStore, persistenceAuditLog, 
                                       playerResolver, serviceResolver, 
                                       new SystemClock()),
                TransitiveLookupWriter.explicitTransitiveLookupWriter(entryStore));
        contentResolver = new DefaultEquivalentContentResolver(new MongoContentResolver(db, entryStore), entryStore);
        
        
        mongoContentPurger = 
                new MongoContentPurger(new MongoContentLister(db), 
                                       new LookupResolvingContentResolver(new MongoContentResolver(db, entryStore), entryStore),
                                       contentWriter, new MongoContentTables(db), db.collection("lookup"), 
                                       TransitiveLookupWriter.explicitTransitiveLookupWriter(entryStore),
                                       TransitiveLookupWriter.generatedTransitiveLookupWriter(entryStore));
    }
    
    @Test
    public void testRemovesContentAndUpdatesEquivalences() {
        Item item1 = testItem(Publisher.BBC, BBC_ITEM_URI);
        Item item2 = testItem(Publisher.METABROADCAST, MB_ITEM_URI);
        
        item2.setEquivalentTo(ImmutableSet.of(LookupRef.from(item1)));
        contentWriter.createOrUpdate(item1);
        contentWriter.createOrUpdate(item2);
        
        LookupEntry lookup = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableSet.of(BBC_ITEM_URI)));
        assertEquals(2, lookup.explicitEquivalents().size());
        
        mongoContentPurger.purge(Publisher.METABROADCAST, ImmutableSet.of(Publisher.BBC));
        
        lookup = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableSet.of(BBC_ITEM_URI)));
        assertEquals(1, lookup.explicitEquivalents().size());
        
        assertEquals(0, contentResolver.resolveUris(ImmutableSet.of(BBC_ITEM_URI), applicationConfiguration(), ImmutableSet.<Annotation>of(), false).get(MB_ITEM_URI).size());
        
        Content queried = Iterables.getOnlyElement(contentResolver.resolveUris(ImmutableSet.of(BBC_ITEM_URI), applicationConfiguration(), ImmutableSet.<Annotation>of(), false).get(BBC_ITEM_URI));
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
        
        Set<Content> content = contentResolver.resolveUris(ImmutableSet.of(BBC_ITEM_URI), applicationConfiguration(), ImmutableSet.<Annotation>of(), false).get(BBC_ITEM_URI);
        assertEquals(2, content.size());
        
        assertEquals(0, Iterables.getFirst(content, null).getAliases().size());
        
    }
    
    private ApplicationConfiguration applicationConfiguration() {
        return ApplicationConfiguration.defaultConfiguration()
                                       .withSource(Publisher.BBC, SourceStatus.AVAILABLE_ENABLED)
                                       .withSource(Publisher.METABROADCAST, SourceStatus.AVAILABLE_ENABLED)
                                       .copyWithPrecedence(ImmutableList.of(Publisher.BBC, Publisher.METABROADCAST));
    }
    
    private Item testItem(Publisher publisher, String uri) {
        Item item = ComplexItemTestDataBuilder.complexItem().withUri(uri).build();
        item.setPublisher(publisher);
        return item;
    }
}
