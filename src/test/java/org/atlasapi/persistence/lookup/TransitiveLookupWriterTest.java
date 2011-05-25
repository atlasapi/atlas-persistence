package org.atlasapi.persistence.lookup;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.entry.Equivalent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class TransitiveLookupWriterTest extends TestCase {

    private LookupEntryStore store = new InMemoryLookupEntryStore();
    private final TransitiveLookupWriter writer = new TransitiveLookupWriter(store);

    // Tests that trivial lookups are written reflexively for all content
    // identifiers
    public void testWriteNewLookup() {

        Item item = createItem("test");

        writer.writeLookup(item, ImmutableSet.<Described> of());

        LookupEntry uriEntry = store.entryFor("testUri");
        assertEquals(item.getCanonicalUri(), uriEntry.id());
        assertEquals(item.getAliases(), uriEntry.aliases());
        assertEquals("testUri", Iterables.getOnlyElement(uriEntry.directEquivalents()).id());

        assertNotNull(uriEntry.created());
        assertNotNull(uriEntry.updated());

        assertEquals(item.getCanonicalUri(), Iterables.getOnlyElement(uriEntry.equivalents()).id());
        assertEquals(item.getPublisher(), Iterables.getOnlyElement(uriEntry.equivalents()).publisher());
        assertEquals(item.getType(), Iterables.getOnlyElement(uriEntry.equivalents()).type());

        LookupEntry aliasEntry = store.entryFor("testAlias");
        assertEquals(Iterables.getOnlyElement(item.getAliases()), aliasEntry.id());
        assertEquals(ImmutableSet.of(), aliasEntry.directEquivalents());

        assertEquals(item.getCanonicalUri(), Iterables.getOnlyElement(aliasEntry.equivalents()).id());
        assertEquals(item.getPublisher(), Iterables.getOnlyElement(aliasEntry.equivalents()).publisher());
        assertEquals(item.getType(), Iterables.getOnlyElement(aliasEntry.equivalents()).type());

        assertNotNull(aliasEntry.created());
        assertNotNull(aliasEntry.updated());

    }

    private Item createItem(String itemName) {
        Item item = new Item(itemName + "Uri", itemName + "Curie", Publisher.BBC);
        item.addAlias(itemName + "Alias");
        return item;
    }

    public void testWriteLookup() {
        Item item1 = createItem("test1");
        Item item2 = createItem("test2");
        Item item3 = createItem("test3");

        // Inserts reflexive entries for items 1, 2, 3
        writer.writeLookup(item1, ImmutableSet.<Described> of());
        writer.writeLookup(item2, ImmutableSet.<Described> of());
        writer.writeLookup(item3, ImmutableSet.<Described> of());

        // Make items 2 and 3 equivalent.
        writer.writeLookup(item2, ImmutableSet.<Described> of(item3));

        hasEquivs("test1Uri", "test1Uri");
        hasDirectEquivs("test1Uri", "test1Uri");
        hasEquivs("test1Alias", "test1Uri");
        hasDirectEquivs("test1Alias");

        hasEquivs("test2Uri", "test2Uri", "test3Uri");
        hasDirectEquivs("test2Uri", "test2Uri", "test3Uri");
        hasEquivs("test2Alias", "test2Uri", "test3Uri");
        hasDirectEquivs("test2Alias");

        hasEquivs("test3Uri", "test2Uri", "test3Uri");
        hasDirectEquivs("test3Uri", "test2Uri", "test3Uri");
        hasEquivs("test3Alias", "test2Uri", "test3Uri");
        hasDirectEquivs("test3Alias");

        // Make items 1 and 2 equivalent, so all three are transitively
        // equivalent
        writer.writeLookup(item1, ImmutableSet.<Described> of(item2));

        hasEquivs("test1Uri", "test2Uri", "test3Uri", "test1Uri");
        hasDirectEquivs("test1Uri", "test1Uri", "test2Uri");
        hasEquivs("test1Alias", "test2Uri", "test3Uri", "test1Uri");
        hasDirectEquivs("test1Alias");

        hasEquivs("test2Uri", "test2Uri", "test3Uri", "test1Uri");
        hasDirectEquivs("test2Uri", "test2Uri", "test3Uri", "test1Uri");
        hasEquivs("test2Alias", "test2Uri", "test3Uri", "test1Uri");
        hasDirectEquivs("test2Alias");

        hasEquivs("test3Uri", "test2Uri", "test3Uri", "test1Uri");
        hasDirectEquivs("test3Uri", "test2Uri", "test3Uri");
        hasEquivs("test3Alias", "test2Uri", "test3Uri", "test1Uri");
        hasDirectEquivs("test3Alias");

        // Make item 1 equivalent to nothing. Item 1 just reflexive, item 2 and
        // 3 still equivalent.
        writer.writeLookup(item1, ImmutableSet.<Described> of());

        hasEquivs("test1Uri", "test1Uri");
        hasDirectEquivs("test1Uri", "test1Uri");
        hasEquivs("test1Alias", "test1Uri");
        hasDirectEquivs("test1Alias");

        hasEquivs("test2Uri", "test2Uri", "test3Uri");
        hasDirectEquivs("test2Uri", "test2Uri", "test3Uri");
        hasEquivs("test2Alias", "test2Uri", "test3Uri");
        hasDirectEquivs("test2Alias");

        hasEquivs("test3Uri", "test2Uri", "test3Uri");
        hasDirectEquivs("test3Uri", "test2Uri", "test3Uri");
        hasEquivs("test3Alias", "test2Uri", "test3Uri");
        hasDirectEquivs("test3Alias");

        // Make 1 and 2 equivalent again.
        writer.writeLookup(item1, ImmutableSet.<Described> of(item2));

        hasEquivs("test1Uri", "test2Uri", "test3Uri", "test1Uri");
        hasDirectEquivs("test1Uri", "test1Uri", "test2Uri");
        hasEquivs("test1Alias", "test2Uri", "test3Uri", "test1Uri");
        hasDirectEquivs("test1Alias");

        hasEquivs("test2Uri", "test2Uri", "test3Uri", "test1Uri");
        hasDirectEquivs("test2Uri", "test2Uri", "test3Uri", "test1Uri");
        hasEquivs("test2Alias", "test2Uri", "test3Uri", "test1Uri");
        hasDirectEquivs("test2Alias");

        hasEquivs("test3Uri", "test2Uri", "test3Uri", "test1Uri");
        hasDirectEquivs("test3Uri", "test2Uri", "test3Uri");
        hasEquivs("test3Alias", "test2Uri", "test3Uri", "test1Uri");
        hasDirectEquivs("test3Alias");

        // Add a new item, 4.
        Item item4 = createItem("test4");
        writer.writeLookup(item4, ImmutableSet.<Described> of());

        // Make 1 equivalent to just 4, instead of 2. 1 and 4 equivalent, 2 and
        // 3 equivalent.
        writer.writeLookup(item1, ImmutableSet.<Described> of(item4));

        hasEquivs("test1Uri", "test1Uri", "test4Uri");
        hasDirectEquivs("test1Uri", "test1Uri", "test4Uri");
        hasEquivs("test1Alias", "test1Uri", "test4Uri");
        hasDirectEquivs("test1Alias");

        hasEquivs("test2Uri", "test2Uri", "test3Uri");
        hasDirectEquivs("test2Uri", "test2Uri", "test3Uri");
        hasEquivs("test2Alias", "test2Uri", "test3Uri");
        hasDirectEquivs("test2Alias");

        hasEquivs("test3Uri", "test2Uri", "test3Uri");
        hasDirectEquivs("test3Uri", "test2Uri", "test3Uri");
        hasEquivs("test3Alias", "test2Uri", "test3Uri");
        hasDirectEquivs("test3Alias");

        hasEquivs("test4Uri", "test1Uri", "test4Uri");
        hasDirectEquivs("test4Uri", "test1Uri", "test4Uri");
        hasEquivs("test4Alias", "test1Uri", "test4Uri");
        hasDirectEquivs("test4Alias");

        // Make all items equivalent.
        writer.writeLookup(item1, ImmutableSet.<Described> of(item3, item4));

        hasEquivs("test1Uri", "test1Uri", "test2Uri", "test3Uri", "test4Uri");
        hasDirectEquivs("test1Uri", "test1Uri", "test3Uri", "test4Uri");
        hasEquivs("test1Alias", "test1Uri", "test2Uri", "test3Uri", "test4Uri");
        hasDirectEquivs("test1Alias");

        hasEquivs("test2Uri", "test1Uri", "test2Uri", "test3Uri", "test4Uri");
        hasDirectEquivs("test2Uri", "test2Uri", "test3Uri");
        hasEquivs("test2Alias", "test1Uri", "test2Uri", "test3Uri", "test4Uri");
        hasDirectEquivs("test2Alias");

        hasEquivs("test3Uri", "test1Uri", "test2Uri", "test3Uri", "test4Uri");
        hasDirectEquivs("test3Uri", "test1Uri", "test2Uri", "test3Uri");
        hasEquivs("test3Alias", "test1Uri", "test2Uri", "test3Uri", "test4Uri");
        hasDirectEquivs("test3Alias");

        hasEquivs("test4Uri", "test1Uri", "test2Uri", "test3Uri", "test4Uri");
        hasDirectEquivs("test4Uri", "test1Uri", "test4Uri");
        hasEquivs("test4Alias", "test1Uri", "test2Uri", "test3Uri", "test4Uri");
        hasDirectEquivs("test4Alias");

    }

    private void hasEquivs(String id, String... transitiveEquivs) {
        assertEquals(ImmutableSet.copyOf(transitiveEquivs), ImmutableSet.copyOf(Iterables.transform(store.entryFor(id).equivalents(), Equivalent.TO_ID)));
    }

    private void hasDirectEquivs(String id, String... directEquivs) {
        assertEquals(ImmutableSet.copyOf(directEquivs), ImmutableSet.copyOf(Iterables.transform(store.entryFor(id).directEquivalents(), Equivalent.TO_ID)));
    }

}
