package org.atlasapi.persistence.lookup;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;

public class MongoLookupWriterTest extends TestCase {

    private final DatabasedMongo db = MongoTestHelper.anEmptyTestDatabase();
    private final MongoLookupWriter writer = new MongoLookupWriter(db);
    private final DBCollection lookup = db.collection("lookup");
    private final LookupEntryTranslator translator = new LookupEntryTranslator();
    
    public void testWriteNewLookup() {

        Item item = createItem("test");
        
        writer.writeLookup(item, ImmutableSet.<Described>of());
        
        LookupEntry uriEntry = translator.fromDbo(lookup.findOne("testUri"));
        assertEquals(item.getCanonicalUri(), uriEntry.id());
        assertEquals(item.getAliases(), uriEntry.aliases());
        
        assertNotNull(uriEntry.created());
        assertNotNull(uriEntry.updated());
        
        assertEquals(item.getCanonicalUri(), Iterables.getOnlyElement(uriEntry.equivalents()).id());
        assertEquals(item.getPublisher(), Iterables.getOnlyElement(uriEntry.equivalents()).publisher());
        assertEquals(item.getType(), Iterables.getOnlyElement(uriEntry.equivalents()).type());
        
        LookupEntry aliasEntry = translator.fromDbo(lookup.findOne("testAlias"));
        assertEquals(Iterables.getOnlyElement(item.getAliases()), aliasEntry.id());
        
        assertEquals(item.getCanonicalUri(), Iterables.getOnlyElement(aliasEntry.equivalents()).id());
        assertEquals(item.getPublisher(), Iterables.getOnlyElement(aliasEntry.equivalents()).publisher());
        assertEquals(item.getType(), Iterables.getOnlyElement(aliasEntry.equivalents()).type());
        
        assertNotNull(aliasEntry.created());
        assertNotNull(aliasEntry.updated());
        
    }

    private Item createItem(String itemName) {
        Item item = new Item(itemName+"Uri", itemName+"Curie", Publisher.BBC);
        item.addAlias(itemName+"Alias");
        return item;
    }
    
    public void testWriteLookup() {
        Item item1 = createItem("test1");
        Item item2 = createItem("test2");
        Item item3 = createItem("test3");
        
        writer.writeLookup(item1, ImmutableSet.<Described>of());
        writer.writeLookup(item2, ImmutableSet.<Described>of());
        writer.writeLookup(item3, ImmutableSet.<Described>of());
        
        writer.writeLookup(item2, ImmutableSet.<Described>of(item3));
        
        hasEquivs("test2Uri", 2);
        hasEquivs("test2Alias", 2);
        hasEquivs("test3Uri", 2);
        hasEquivs("test3Alias", 2);
        
        writer.writeLookup(item1, ImmutableSet.<Described>of(item2));
        
        hasEquivs("test1Uri", 3);
        hasEquivs("test1Alias", 3);
        hasEquivs("test2Uri", 3);
        hasEquivs("test2Alias", 3);
        hasEquivs("test3Uri", 3);
        hasEquivs("test3Alias", 3);
        
        writer.writeLookup(item1, ImmutableSet.<Described>of());
        
        hasEquivs("test1Uri", 1);
        hasEquivs("test1Alias", 1);
        hasEquivs("test2Uri", 2);
        hasEquivs("test2Alias", 2);
        hasEquivs("test3Uri", 2);
        hasEquivs("test3Alias", 2);
        
        writer.writeLookup(item1, ImmutableSet.<Described>of(item2));

        hasEquivs("test1Uri", 3);
        hasEquivs("test1Alias", 3);
        hasEquivs("test2Uri", 3);
        hasEquivs("test2Alias", 3);
        hasEquivs("test3Uri", 3);
        hasEquivs("test3Alias", 3);
        
        Item item4 = createItem("test4");
        
        writer.writeLookup(item4, ImmutableSet.<Described>of());
        
        writer.writeLookup(item1, ImmutableSet.<Described>of(item4));
        
        hasEquivs("test1Uri", 2);
        hasEquivs("test1Alias", 2);
        hasEquivs("test2Uri", 2);
        hasEquivs("test2Alias", 2);
        hasEquivs("test3Uri", 2);
        hasEquivs("test3Alias", 2);
        hasEquivs("test4Uri", 2);
        hasEquivs("test4Alias", 2);
    }

    private void hasEquivs(String id, int equivs) {
        LookupEntry uri1Entry = translator.fromDbo(lookup.findOne(id));
        assertEquals(equivs, uri1Entry.equivalents().size());
    }

}
