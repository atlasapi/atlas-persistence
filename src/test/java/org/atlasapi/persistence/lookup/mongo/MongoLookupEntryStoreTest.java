package org.atlasapi.persistence.lookup.mongo;

import static com.google.common.base.Predicates.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.BasicDBObject;

public class MongoLookupEntryStoreTest {

    private static DatabasedMongo mongo;
    private static MongoLookupEntryStore entryStore;
    
    @BeforeClass
    public static void setUp() {
        mongo = MongoTestHelper.anEmptyTestDatabase();
        entryStore = new MongoLookupEntryStore(mongo);
    }
    
    @After 
    public void clear() {
        mongo.collection("lookup").remove(new BasicDBObject());
    }
    
    @Test
    public void testStore() {

        Item testItem = new Item("testItemUri", "testItemCurie", Publisher.BBC);
        testItem.addAlias("testItemAlias");
        
        LookupEntry testEntry = LookupEntry.lookupEntryFrom(testItem);
        entryStore.store(testEntry);
        
        Iterable<LookupEntry> uriEntry = entryStore.entriesForUris(ImmutableList.of("testItemUri"));
        assertEquals(testEntry, Iterables.getOnlyElement(uriEntry));
        
        assertFalse(Iterables.isEmpty(entryStore.entriesForUris(ImmutableList.of("testItemAlias"))));
    }

    @Test
    public void testEnsureLookup() {

        Item testItem = new Item("newItemUri", "newItemCurie", Publisher.BBC);
        testItem.addAlias("newItemAlias");
        
        entryStore.ensureLookup(testItem);
        
        LookupEntry firstEntry = Iterables.getOnlyElement(entryStore.entriesForUris(ImmutableList.of("newItemUri")));
        
        assertNotNull(firstEntry);
        
        entryStore.ensureLookup(testItem);
        
        assertEquals(firstEntry.created(), Iterables.getOnlyElement(entryStore.entriesForUris(ImmutableList.of("newItemUri"))).created());
    }
    
    @Test
    public void testEnsureLookupWritesEntryWhenOfDifferentType() {
        
        Item testItem = new Item("oldItemUri", "oldItemCurie", Publisher.BBC);
        testItem.addAlias("oldItemAlias");
        
        entryStore.ensureLookup(testItem);
        
        LookupEntry firstEntry = Iterables.getOnlyElement(entryStore.entriesForUris(ImmutableList.of("oldItemUri")));
        
        Item transitiveItem = new Item("transitiveUri", "transitiveCurie", Publisher.PA);
        transitiveItem.addAlias("transitiveAlias");
        
        entryStore.ensureLookup(transitiveItem);
        
        LookupEntry secondEntry = Iterables.getOnlyElement(entryStore.entriesForUris(ImmutableList.of("transitiveUri")));
        
        ImmutableSet<LookupRef> secondRef = ImmutableSet.of(secondEntry.lookupRef());
        firstEntry = firstEntry.copyWithDirectEquivalents(secondRef).copyWithEquivalents(secondRef);
        ImmutableSet<LookupRef> firstRef = ImmutableSet.of(firstEntry.lookupRef());
        secondEntry= secondEntry.copyWithDirectEquivalents(firstRef).copyWithEquivalents(firstRef);

        entryStore.store(firstEntry);
        entryStore.store(secondEntry);

        Episode testEpisode = new Episode("oldItemUri", "oldItemCurie", Publisher.BBC);
        testEpisode.addAlias("oldItemAlias");
        testEpisode.setParentRef(new ParentRef("aBrand"));
        
        entryStore.ensureLookup(testEpisode);

        LookupEntry newEntry = Iterables.getOnlyElement(entryStore.entriesForUris(ImmutableList.of("oldItemUri")));
        assertEquals(ContentCategory.CHILD_ITEM, newEntry.lookupRef().category());
        assertTrue(newEntry.directEquivalents().contains(secondEntry.lookupRef()));
        assertTrue(newEntry.equivalents().contains(secondEntry.lookupRef()));
        assertEquals(firstEntry.created(), newEntry.created());
        
        LookupEntry aliasEntry = Iterables.getOnlyElement(entryStore.entriesForUris(ImmutableList.of("oldItemAlias")));
        assertEquals(ContentCategory.CHILD_ITEM, aliasEntry.lookupRef().category());
        assertTrue(aliasEntry.equivalents().contains(secondEntry.lookupRef()));
        assertEquals(firstEntry.created(), aliasEntry.created());
        
        LookupEntry transtiveEntry = Iterables.getOnlyElement(entryStore.entriesForUris(ImmutableList.of("transitiveUri")));
        assertEquals(ContentCategory.CHILD_ITEM, Iterables.find(transtiveEntry.equivalents(), equalTo(newEntry.lookupRef())).category());
        assertEquals(ContentCategory.CHILD_ITEM, Iterables.find(transtiveEntry.directEquivalents(), equalTo(newEntry.lookupRef())).category());
        assertEquals(transtiveEntry.created(), secondEntry.created());
        
        LookupEntry aliasTranstiveEntry = Iterables.getOnlyElement(entryStore.entriesForUris(ImmutableList.of("transitiveAlias")));
        assertEquals(ContentCategory.CHILD_ITEM, Iterables.find(aliasTranstiveEntry.equivalents(), equalTo(newEntry.lookupRef())).category());
    }
    
    @Test
    public void testEnsureLookupChangesTypeForNonTopLevelSeries() {
        
        Series series = new Series("seriesUri","seriesCurie", Publisher.BBC);
        
        entryStore.ensureLookup(series);
        
        LookupEntry newEntry = Iterables.getOnlyElement(entryStore.entriesForUris(ImmutableList.of(series.getCanonicalUri())));
        
        assertEquals(ContentCategory.CONTAINER, newEntry.lookupRef().category());
        
        series = new Series("seriesUri","seriesCurie", Publisher.BBC);
        series.setParent(new Brand("brandUri","brandCurie",Publisher.BBC));

        entryStore.ensureLookup(series);
        
        LookupEntry changedEntry = Iterables.getOnlyElement(entryStore.entriesForUris(ImmutableList.of(series.getCanonicalUri())));
        
        assertEquals(ContentCategory.PROGRAMME_GROUP, changedEntry.lookupRef().category());
    }
}
