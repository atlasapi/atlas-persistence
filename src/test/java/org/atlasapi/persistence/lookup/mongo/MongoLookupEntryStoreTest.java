package org.atlasapi.persistence.lookup.mongo;

import static com.google.common.base.Predicates.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.persistence.mongo.MongoUpdateBuilder;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.ReadPreference;

@RunWith( MockitoJUnitRunner.class )
public class MongoLookupEntryStoreTest {

    private static DatabasedMongo mongo;
    private static MongoLookupEntryStore entryStore;
    private static Logger log = mock(Logger.class);
    private static DBCollection collection; 
    
    @BeforeClass
    public static void setUp() {
        mongo = MongoTestHelper.anEmptyTestDatabase();
        collection = mongo.collection("lookup");
        entryStore = new MongoLookupEntryStore(
                            collection, 
                            ReadPreference.primary(), 
                            new NoLoggingPersistenceAuditLog(), 
                            log
                         );
    }
    
    @After 
    public void clear() {
        mongo.collection("lookup").remove(new BasicDBObject());
        reset(log);
    }
    
    @Test
    public void testStore() {

        Item testItemOne = new Item("testItemOneUri", "testItem1Curie", Publisher.BBC);
        testItemOne.addAliasUrl("testItemOneAlias");
        testItemOne.addAlias(new Alias("testItemOneNamespace", "testItemOneValue"));
        testItemOne.addAliasUrl("sharedAlias");
        testItemOne.addAlias(new Alias("sharedNamespace", "sharedValue"));
        
        Item testItemTwo = new Item("testItemTwoUri", "testItem2Curie", Publisher.BBC);
        testItemTwo.addAliasUrl("testItemTwoAlias");
        testItemTwo.addAlias(new Alias("testItemTwoNamespace", "testItemTwoValue"));
        testItemTwo.addAliasUrl("sharedAlias");
        testItemTwo.addAlias(new Alias("sharedNamespace", "sharedValue"));
        
        LookupEntry testEntryOne = LookupEntry.lookupEntryFrom(testItemOne);
        LookupEntry testEntryTwo = LookupEntry.lookupEntryFrom(testItemTwo);
        entryStore.store(testEntryOne);
        entryStore.store(testEntryTwo);
        
        Iterable<LookupEntry> uriEntry = entryStore.entriesForCanonicalUris(ImmutableList.of("testItemOneUri"));
        assertEquals(testEntryOne, Iterables.getOnlyElement(uriEntry));
        
        //Shouldn't be able to find entry by canonical URI using alias
        Iterable<LookupEntry> aliasEntry = entryStore.entriesForCanonicalUris(ImmutableList.of("testItemOneAlias"));
        assertTrue("Found entry by canonical URI using alias", Iterables.isEmpty(aliasEntry));

        aliasEntry = entryStore.entriesForIdentifiers(ImmutableList.of("testItemOneAlias"), true);
        assertEquals(testEntryOne, Iterables.getOnlyElement(aliasEntry));
        
        aliasEntry = entryStore.entriesForAliases(Optional.of("testItemOneNamespace"), ImmutableList.of("testItemOneValue"));
        assertEquals(testEntryOne, Iterables.getOnlyElement(aliasEntry));
        
        aliasEntry = entryStore.entriesForAliases(Optional.<String>absent(), ImmutableList.of("testItemOneValue"));
        assertEquals(testEntryOne, Iterables.getOnlyElement(aliasEntry));
        
        uriEntry = entryStore.entriesForCanonicalUris(ImmutableList.of("testItemTwoUri"));
        assertEquals(testEntryTwo, Iterables.getOnlyElement(uriEntry));
        
        //Shouldn't be able to find entry by canonical URI using alias
        aliasEntry = entryStore.entriesForCanonicalUris(ImmutableList.of("testItemTwoAlias"));
        assertTrue("Found entry by canonical URI using alias", Iterables.isEmpty(aliasEntry));
        
        aliasEntry = entryStore.entriesForIdentifiers(ImmutableList.of("sharedAlias"), true);
        LookupEntry first = Iterables.get(aliasEntry, 0);
        LookupEntry second = Iterables.get(aliasEntry, 1);
        assertThat(first, isOneOf(testEntryOne, testEntryTwo));
        assertThat(second, isOneOf(testEntryOne, testEntryTwo));
        assertThat(first, is(not(second)));
        
        aliasEntry = entryStore.entriesForAliases(Optional.of("sharedNamespace"), ImmutableList.of("sharedValue"));
        first = Iterables.get(aliasEntry, 0);
        second = Iterables.get(aliasEntry, 1);
        assertThat(first, isOneOf(testEntryOne, testEntryTwo));
        assertThat(second, isOneOf(testEntryOne, testEntryTwo));
        assertThat(first, is(not(second)));
        
        aliasEntry = entryStore.entriesForAliases(Optional.<String>absent(), ImmutableList.of("sharedValue"));
        first = Iterables.get(aliasEntry, 0);
        second = Iterables.get(aliasEntry, 1);
        assertThat(first, isOneOf(testEntryOne, testEntryTwo));
        assertThat(second, isOneOf(testEntryOne, testEntryTwo));
        assertThat(first, is(not(second)));
    }

    @Test
    public void testEnsureLookup() {

        Item testItem = new Item("newItemUri", "newItemCurie", Publisher.BBC);
        testItem.addAliasUrl("newItemAlias");
        
        entryStore.ensureLookup(testItem);
        
        LookupEntry firstEntry = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableList.of("newItemUri")));
        
        assertNotNull(firstEntry);
        
        entryStore.ensureLookup(testItem);
        
        assertEquals(firstEntry.created(), Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableList.of("newItemUri"))).created());
    }
    
    @Test
    public void testEnsureLookupWritesEntryWhenOfDifferentType() {
        
        Item testItem = new Item("oldItemUri", "oldItemCurie", Publisher.BBC);
        testItem.addAliasUrl("oldItemAlias");
        
        entryStore.ensureLookup(testItem);
        
        LookupEntry firstEntry = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableList.of("oldItemUri")));
        
        Item transitiveItem = new Item("transitiveUri", "transitiveCurie", Publisher.PA);
        transitiveItem.addAliasUrl("transitiveAlias");
        
        entryStore.ensureLookup(transitiveItem);
        
        LookupEntry secondEntry = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableList.of("transitiveUri")));
        
        ImmutableSet<LookupRef> secondRef = ImmutableSet.of(secondEntry.lookupRef());
        firstEntry = firstEntry.copyWithDirectEquivalents(secondRef).copyWithEquivalents(secondRef);
        ImmutableSet<LookupRef> firstRef = ImmutableSet.of(firstEntry.lookupRef());
        secondEntry= secondEntry.copyWithDirectEquivalents(firstRef).copyWithEquivalents(firstRef);

        entryStore.store(firstEntry);
        entryStore.store(secondEntry);

        Episode testEpisode = new Episode("oldItemUri", "oldItemCurie", Publisher.BBC);
        testEpisode.addAliasUrl("oldItemAlias");
        testEpisode.setParentRef(new ParentRef("aBrand"));
        
        entryStore.ensureLookup(testEpisode);

        LookupEntry newEntry = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableList.of("oldItemUri")));
        assertEquals(ContentCategory.CHILD_ITEM, newEntry.lookupRef().category());
        assertTrue(newEntry.directEquivalents().contains(secondEntry.lookupRef()));
        assertTrue(newEntry.equivalents().contains(secondEntry.lookupRef()));
        assertEquals(firstEntry.created(), newEntry.created());
        
        assertTrue(Iterables.isEmpty(entryStore.entriesForCanonicalUris(ImmutableList.of("oldItemAlias"))));
        
        LookupEntry transtiveEntry = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableList.of("transitiveUri")));
        assertEquals(ContentCategory.CHILD_ITEM, Iterables.find(transtiveEntry.equivalents(), equalTo(newEntry.lookupRef())).category());
        assertEquals(ContentCategory.CHILD_ITEM, Iterables.find(transtiveEntry.directEquivalents(), equalTo(newEntry.lookupRef())).category());
        assertEquals(transtiveEntry.created(), secondEntry.created());
        
        assertTrue(Iterables.isEmpty(entryStore.entriesForCanonicalUris(ImmutableList.of("transitiveAlias"))));
    }
    
    @Test
    public void testEnsureLookupChangesTypeForNonTopLevelSeries() {
        
        Series series = new Series("seriesUri","seriesCurie", Publisher.BBC);
        
        entryStore.ensureLookup(series);
        
        LookupEntry newEntry = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableList.of(series.getCanonicalUri())));
        
        assertEquals(ContentCategory.CONTAINER, newEntry.lookupRef().category());
        
        series = new Series("seriesUri","seriesCurie", Publisher.BBC);
        series.setParent(new Brand("brandUri","brandCurie",Publisher.BBC));

        entryStore.ensureLookup(series);
        
        LookupEntry changedEntry = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableList.of(series.getCanonicalUri())));
        
        assertEquals(ContentCategory.PROGRAMME_GROUP, changedEntry.lookupRef().category());
    }
    
    @Test
    public void testEnsureLookupRewritesEntryWhenAliasesChange() {
        
        Brand brand = new Brand("brandUri", "brandCurie", Publisher.BBC_REDUX);
        brand.addAliasUrl("brandAlias");
        
        entryStore.ensureLookup(brand);
        
        brand.addAliasUrl("anotherBrandAlias");
        
        entryStore.ensureLookup(brand);

        LookupEntry entry = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableList.of(brand.getCanonicalUri())));
        
        assertThat(entry.aliasUrls().size(), is(3));
        assertThat(entry.aliasUrls(), hasItems("brandUri", "brandAlias", "anotherBrandAlias"));

        brand.setAliasUrls(ImmutableSet.of("anotherBrandAlias"));

        entryStore.ensureLookup(brand);
        
        entry = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableList.of(brand.getCanonicalUri())));
        
        assertThat(entry.aliasUrls().size(), is(2));
        assertThat(entry.aliasUrls(), hasItems("brandUri", "anotherBrandAlias"));
        
        
    }
    
    // Ensure that for a lookup with 2 aliases :
    // {
    //      namespace : "a",
    //      value : "b"
    // },
    // {
    //      namespace : "c",
    //      value : "d"
    // },
    // a lookup for namespace = "a" and value = "b" does not succeed, i.e. lookup both values together
    @Test
    public void testEnsureMatchingNamespaceValueLookup() {
        Item testItemOne = new Item("testItemOneUri", "testItem1Curie", Publisher.BBC);
        testItemOne.addAlias(new Alias("a", "b"));
        testItemOne.addAlias(new Alias("c", "d"));
        
        LookupEntry testEntryOne = LookupEntry.lookupEntryFrom(testItemOne);
        entryStore.store(testEntryOne);
        
        Iterable<LookupEntry> aliasEntry = entryStore.entriesForAliases(Optional.of("a"), ImmutableList.of("b"));
        assertEquals(testEntryOne, Iterables.getOnlyElement(aliasEntry));
        
        aliasEntry = entryStore.entriesForAliases(Optional.of("a"), ImmutableList.of("d"));
        assertTrue(Iterables.isEmpty(aliasEntry));
        
        aliasEntry = entryStore.entriesForAliases(Optional.of("c"), ImmutableList.of("b"));
        assertTrue(Iterables.isEmpty(aliasEntry));
        
        aliasEntry = entryStore.entriesForAliases(Optional.of("c"), ImmutableList.of("d"));
        assertEquals(testEntryOne, Iterables.getOnlyElement(aliasEntry));
    }
    
    @Test
    public void testMultipleValueLookup() {
        Item testItemOne = new Item("testItemOneUri", "testItem1Curie", Publisher.BBC);
        testItemOne.addAlias(new Alias("namespace", "valueOne"));
        
        Item testItemTwo = new Item("testItemTwoUri", "testItem2Curie", Publisher.BBC);
        testItemTwo.addAlias(new Alias("namespace", "valueTwo"));
        
        LookupEntry testEntryOne = LookupEntry.lookupEntryFrom(testItemOne);
        LookupEntry testEntryTwo = LookupEntry.lookupEntryFrom(testItemTwo);
        entryStore.store(testEntryOne);
        entryStore.store(testEntryTwo);
        
        Iterable<LookupEntry> aliasEntry = entryStore.entriesForAliases(Optional.of("namespace"), ImmutableList.of("valueOne", "valueTwo"));
        LookupEntry first = Iterables.get(aliasEntry, 0);
        LookupEntry second = Iterables.get(aliasEntry, 1);
        assertThat(first, isOneOf(testEntryOne, testEntryTwo));
        assertThat(second, isOneOf(testEntryOne, testEntryTwo));
        assertThat(first, is(not(second)));

        aliasEntry = entryStore.entriesForAliases(Optional.<String>absent(), ImmutableList.of("valueOne", "valueTwo"));
        first = Iterables.get(aliasEntry, 0);
        second = Iterables.get(aliasEntry, 1);
        assertThat(first, isOneOf(testEntryOne, testEntryTwo));
        assertThat(second, isOneOf(testEntryOne, testEntryTwo));
        assertThat(first, is(not(second)));        
    }
    
    @Test
    public void testLookupIdsByUri() {
        Item testItemOne = new Item("testItemOneUri", "testItem1Curie", Publisher.BBC);
        testItemOne.setId(1L);
        Item testItemTwo = new Item("testItemTwoUri", "testItem2Curie", Publisher.BBC);
        testItemTwo.setId(2L);
        Item testItemThree = new Item("testItemThreeUri", "testItem3Curie", Publisher.BBC);
        
        entryStore.store(LookupEntry.lookupEntryFrom(testItemOne));
        entryStore.store(LookupEntry.lookupEntryFrom(testItemTwo));
        entryStore.store(LookupEntry.lookupEntryFrom(testItemThree));
        
        Map<String, Long> idsForCanonicalUris = entryStore.idsForCanonicalUris(
                ImmutableSet.of(testItemOne.getCanonicalUri(), testItemTwo.getCanonicalUri(), testItemThree.getCanonicalUri()));
        assertThat(idsForCanonicalUris.get(testItemOne.getCanonicalUri()), is(testItemOne.getId()));
        assertThat(idsForCanonicalUris.get(testItemTwo.getCanonicalUri()), is(testItemTwo.getId()));
        assertThat(idsForCanonicalUris.size(), is(2));
    }
    
    @Test
    // This isn't an ideal way of asserting the correct behaviour, but as
    // a DbCollection isn't an interface, we can't mock it out. 
    //
    // I decided to add a marker field to confirm that a second save() is
    // not performed. This is slightly fragile as if we change the 
    // implementation in future to do an update() then we'd not catch a
    // rogue save, so I decided to also check the log messages. String checking
    // is obviously quite fragile but solves the case of a future change to
    // using update()
    public void testNoWriteIfEntrySame() {
        String uri = "testItemOneUri";
        Item testItemOne = new Item(uri, "testItem1Curie", Publisher.BBC);
        testItemOne.setId(1L);
        
        entryStore.store(LookupEntry.lookupEntryFrom(testItemOne));
        
        String MARKER_FIELD = "not_updated";
        String MARKER_VALUE = "x";
        collection.update(
                new MongoQueryBuilder().idEquals(uri).build(), 
                new MongoUpdateBuilder().setField(MARKER_FIELD, MARKER_VALUE).build()
        );
        
        entryStore.store(LookupEntry.lookupEntryFrom(testItemOne));
        
        ArgumentCaptor<String> arg1captor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> arg2captor = ArgumentCaptor.forClass(String.class);
        
        verify(log, atLeastOnce()).debug(arg1captor.capture(), arg2captor.capture());
        
        assertEquals(
                MARKER_VALUE,
                collection.findOne(new MongoQueryBuilder().idEquals(uri).build()).get(MARKER_FIELD)
        );
        
        assertEquals(
                ImmutableList.of("New entry or hash code changed for URI {}; writing", 
                                 "Hash code not changed for URI {}; skipping write"), 
                arg1captor.getAllValues()
        );
    }
    
    @Test
    public void testEnsureLookupWritesWhenActivelyPublishedChanges() {
        
        Item item = new Item("http://example.org/item", "testItem1Curie", Publisher.BBC);
        
        entryStore.ensureLookup(item);
        LookupEntry entry = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableList.of(item.getCanonicalUri())));
        assertTrue(entry.activelyPublished());
        
        item.setActivelyPublished(false);
        entryStore.ensureLookup(item);

        entry = Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableList.of(item.getCanonicalUri())));
        assertFalse(entry.activelyPublished());
    }

    @Test
    public void testEntriesForPublishersReturnsItemsForRequestedPublishers() throws Exception {
        LookupEntry shouldReturnFirst = LookupEntry.lookupEntryFrom(
                new Item("uriA", "uriA", Publisher.BBC)
        );
        LookupEntry shouldReturnSecond = LookupEntry.lookupEntryFrom(
                new Item("uriB", "uriB", Publisher.BBC)
        );
        LookupEntry shouldNotReturn = LookupEntry.lookupEntryFrom(
                new Item("uriC", "uriC", Publisher.METABROADCAST)
        );

        entryStore.store(shouldReturnFirst);
        entryStore.store(shouldReturnSecond);
        entryStore.store(shouldNotReturn);

        Iterable<LookupEntry> entries = entryStore.entriesForPublishers(
                ImmutableList.of(Publisher.BBC), ContentListingProgress.START, true
        );

        assertThat(Iterables.size(entries), is(2));
        assertThat(Iterables.get(entries, 0).uri(), is(shouldReturnFirst.uri()));
        assertThat(Iterables.get(entries, 1).uri(), is(shouldReturnSecond.uri()));
    }

    @Test
    public void testEntriesForPublishersReturnsNonPublishedItemsWhenRequested() throws Exception {
        LookupEntry published = LookupEntry.lookupEntryFrom(
                new Item("uriA", "uriA", Publisher.BBC)
        );
        Item unpublishedItem = new Item("uriB", "uriB", Publisher.BBC);
        unpublishedItem.setActivelyPublished(false);
        LookupEntry unpublished = LookupEntry.lookupEntryFrom(unpublishedItem);

        entryStore.store(published);
        entryStore.store(unpublished);

        Iterable<LookupEntry> entries = entryStore.entriesForPublishers(
                ImmutableList.of(Publisher.BBC), ContentListingProgress.START, false
        );

        assertThat(Iterables.size(entries), is(2));
        assertThat(Iterables.get(entries, 0).uri(), is(published.uri()));
        assertThat(Iterables.get(entries, 1).uri(), is(unpublished.uri()));
    }

    @Test
    public void testEntriesForPublishersDoesNotReturnNonPublishedItemsIfNotRequested()
            throws Exception {
        LookupEntry published = LookupEntry.lookupEntryFrom(
                new Item("uriA", "uriA", Publisher.BBC)
        );
        Item unpublishedItem = new Item("uriB", "uriB", Publisher.BBC);
        unpublishedItem.setActivelyPublished(false);
        LookupEntry unpublished = LookupEntry.lookupEntryFrom(unpublishedItem);

        entryStore.store(published);
        entryStore.store(unpublished);

        Iterable<LookupEntry> entries = entryStore.entriesForPublishers(
                ImmutableList.of(Publisher.BBC), ContentListingProgress.START, true
        );

        assertThat(Iterables.size(entries), is(1));
        assertThat(Iterables.get(entries, 0).uri(), is(published.uri()));
    }

    @Test
    public void testEntriesForPublishersWithProgressReturnsRemainingItems() throws Exception {
        LookupEntry first = LookupEntry.lookupEntryFrom(
                new Item("uriA", "uriA", Publisher.BBC)
        );
        LookupEntry second = LookupEntry.lookupEntryFrom(
                new Item("uriB", "uriB", Publisher.BBC)
        );
        Item lastOneProcessed = new Item("uriC", "uriC", Publisher.METABROADCAST);
        LookupEntry third = LookupEntry.lookupEntryFrom(lastOneProcessed);
        LookupEntry fourth = LookupEntry.lookupEntryFrom(
                new Item("uriD", "uriD", Publisher.METABROADCAST)
        );
        LookupEntry fifth = LookupEntry.lookupEntryFrom(
                new Item("uriE", "uriE", Publisher.CANARY)
        );

        entryStore.store(first);
        entryStore.store(second);
        entryStore.store(third);
        entryStore.store(fourth);
        entryStore.store(fifth);

        ContentListingProgress progress = ContentListingProgress.progressFrom(lastOneProcessed);

        Iterable<LookupEntry> entries = entryStore.entriesForPublishers(
                ImmutableList.of(Publisher.BBC, Publisher.METABROADCAST, Publisher.CANARY),
                progress,
                true
        );

        assertThat(Iterables.size(entries), is(2));
        assertThat(Iterables.get(entries, 0).uri(), is(fourth.uri()));
        assertThat(Iterables.get(entries, 1).uri(), is(fifth.uri()));
    }
}
