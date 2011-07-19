package org.atlasapi.persistence.content.mongo;

import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_CONTAINERS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_ITEMS;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentListingHandler;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.SystemClock;

public class MongoContentListerTest {

    private static final Item item1 = new Item("item1","item1curie",Publisher.BBC);
    private static final Item item2 = new Item("item2", "item2curie",Publisher.C4);
    
    private static final Brand brand= new Brand("brand1", "brand2curie", Publisher.BBC);
    
    private static final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    private static final MongoContentTables contentTables = new MongoContentTables(mongo);
    
    private final MongoContentLister lister = new MongoContentLister(contentTables);
    
    @BeforeClass
    public static void writeTestContents() {
        NewLookupWriter lookupStore = new NewLookupWriter() {
            @Override
            public void ensureLookup(Described described) {
            }
        };
        MongoContentWriter writer = new MongoContentWriter(mongo, lookupStore , new SystemClock());
        writer.createOrUpdate(brand);
        writer.createOrUpdate(item1);
        writer.createOrUpdate(item2);
    }
    
    private ContentListingHandler storingContentListingHandler(final List<Content> allContents) {
        return new ContentListingHandler() {
            @Override
            public boolean handle(Iterable<? extends Content> contents, ContentListingProgress progress) {
                Iterables.addAll(allContents, contents);
                assertTrue(progress.count() > 0);
                assertTrue(progress.total() > 0);
                return true;
            }
        };
    }
    
    @Test
    public void testListContentFromOneTable() {
        
        final List<Content> contents = Lists.newArrayList();
        ContentListingHandler handler = storingContentListingHandler(contents);
        
        lister.listContent(ImmutableSet.of(TOP_LEVEL_ITEMS), defaultCriteria(), handler);
        
        assertEquals(ImmutableList.of(item1, item2), contents);
        
    }
    
    @Test
    public void testListContentFromTwoTables() {
        
        final List<Content> contents = Lists.newArrayList();
        ContentListingHandler handler = storingContentListingHandler(contents);
        
        lister.listContent(ImmutableSet.of(TOP_LEVEL_ITEMS, TOP_LEVEL_CONTAINERS), defaultCriteria(), handler);
        
        assertEquals(ImmutableList.of(brand, item1, item2), contents);
        
    }
    
    @Test
    public void testStopsListContentWhenHandlerReturnsFalse() {
        
        final List<Content> processedContents = Lists.newArrayList();

        ContentListingHandler handler = new ContentListingHandler() {
            @Override
            public boolean handle(Iterable<? extends Content> contents, ContentListingProgress progress) {
                for (Content content : contents) {
                    if (content.getCanonicalUri().equals("item1")) {
                        return false;
                    } else {
                        processedContents.add(content);
                    }
                }
                return true;
            }
        };
        
        boolean finished = lister.listContent(ImmutableSet.of(TOP_LEVEL_ITEMS, TOP_LEVEL_CONTAINERS), defaultCriteria(), handler);
        
        assertEquals(ImmutableList.of(brand), processedContents);
        assertFalse(finished);
    }

    @Test
    public void testRestartListing() {
        
        final List<Content> contents = Lists.newArrayList();
        ContentListingHandler handler = storingContentListingHandler(contents);
        
        ContentListingProgress progress = new ContentListingProgress("item1", TOP_LEVEL_ITEMS);
        boolean finished = lister.listContent(ImmutableSet.of(TOP_LEVEL_ITEMS, TOP_LEVEL_CONTAINERS), defaultCriteria().startingAt(progress), handler);
        
        assertEquals(ImmutableList.of(item2), contents);
        assertTrue(finished);
        
    }
}
