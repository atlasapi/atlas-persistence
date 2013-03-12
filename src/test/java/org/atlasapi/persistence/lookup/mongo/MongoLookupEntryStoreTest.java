package org.atlasapi.persistence.lookup.mongo;

import static com.google.common.base.Predicates.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.atlasapi.media.entity.Alias;
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

import com.google.common.base.Optional;
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
        entryStore = new MongoLookupEntryStore(mongo.collection("lookup"));
    }
    
    @After 
    public void clear() {
        mongo.collection("lookup").remove(new BasicDBObject());
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
}
