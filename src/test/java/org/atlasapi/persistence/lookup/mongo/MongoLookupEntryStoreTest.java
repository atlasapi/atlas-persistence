package org.atlasapi.persistence.lookup.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

public class MongoLookupEntryStoreTest {

    private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    private final MongoLookupEntryStore entryStore = new MongoLookupEntryStore(mongo);
    
    @Test
    public void testStore() {

        Item testItem = new Item("testItemUri", "testItemCurie", Publisher.BBC);
        testItem.addAlias("testItemAlias");
        
        LookupEntry testEntry = LookupEntry.lookupEntryFrom(testItem);
        entryStore.store(testEntry);
        
        Iterable<LookupEntry> uriEntry = entryStore.entriesFor(ImmutableList.of("testItemUri"));
        assertEquals(testEntry, Iterables.getOnlyElement(uriEntry));
        
        assertFalse(Iterables.isEmpty(entryStore.entriesFor(ImmutableList.of("testItemAlias"))));
    }

    @Test
    public void testEnsureLookup() {

        Item testItem = new Item("newItemUri", "newItemCurie", Publisher.BBC);
        testItem.addAlias("newItemAlias");
        
        entryStore.ensureLookup(testItem);
        
        LookupEntry firstEntry = Iterables.getOnlyElement(entryStore.entriesFor(ImmutableList.of("newItemUri")));
        
        assertNotNull(firstEntry);
        
        entryStore.ensureLookup(testItem);
        
        assertEquals(firstEntry.created(), Iterables.getOnlyElement(entryStore.entriesFor(ImmutableList.of("newItemUri"))).created());
    }
}
