package org.atlasapi.persistence.lookup.cassandra;

import static org.junit.Assert.assertEquals;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.atlasapi.persistence.cassandra.BaseCassandraTest;
import org.junit.Ignore;

@Ignore(value = "Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraLookupEntryStoreTest extends BaseCassandraTest {
    
    private CassandraLookupEntryStore entryStore;
    
    @Override
    @Before
    public void before() {
        super.before();
        Logger.getRootLogger().setLevel(Level.INFO);
        Logger.getRootLogger().addAppender(new ConsoleAppender(new PatternLayout("%-6r [%p] %c - %m%n")));
        entryStore = new CassandraLookupEntryStore(context, 1000);
    }
    
    @Test
    public void testStoreAndReadLookupEntry() {

        Item testItem = new Item("testItemUri", "testItemCurie", Publisher.BBC);
        testItem.addAlias("testItemAlias");
        
        LookupEntry testEntry = LookupEntry.lookupEntryFrom(testItem);
        entryStore.store(testEntry);
        
        Iterable<LookupEntry> uriEntry = entryStore.entriesForCanonicalUris(ImmutableList.of("testItemUri"));
        assertEquals(testEntry, Iterables.getOnlyElement(uriEntry));
        
    }

    @Test
    public void testStoreAndReadLookupEntriesWithAndWithoutAliases() {
        
        Item testItem2 = new Item("testItemUri2", "testItemCurie2", Publisher.BBC);
        testItem2.addAlias("testItemAlias2");
        testItem2.addAlias("sharedAlias");
        
        Item testItem3 = new Item("testItemUri3", "testItemCurie3", Publisher.BBC);
        testItem3.addAlias("testItemAlias3");
        testItem3.addAlias("sharedAlias");
        
        entryStore.ensureLookup(testItem2);
        entryStore.ensureLookup(testItem3);
        
        Iterable<LookupEntry> entries = entryStore.entriesForIdentifiers(ImmutableList.of("testItemAlias2"), true);
        assertEquals(testItem2.getCanonicalUri(), Iterables.getOnlyElement(entries).uri());
        
        entries = entryStore.entriesForIdentifiers(ImmutableList.of("testItemAlias3"), true);
        assertEquals(testItem3.getCanonicalUri(), Iterables.getOnlyElement(entries).uri());
        
        entries = entryStore.entriesForIdentifiers(ImmutableList.of("sharedAlias"), true);
        assertEquals(2, Iterables.size(entries));
        
        entries = entryStore.entriesForIdentifiers(ImmutableList.of("testItemUri2"), false);
        assertEquals(1, Iterables.size(entries));
        
    }

}
