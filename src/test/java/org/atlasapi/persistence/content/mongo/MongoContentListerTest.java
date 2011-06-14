package org.atlasapi.persistence.content.mongo;

import java.util.List;



import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentListingHandler;
import org.atlasapi.persistence.content.ContentListingProgress;
import org.atlasapi.persistence.content.ContentTable;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.junit.Test;
import static org.junit.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.SystemClock;

public class MongoContentListerTest {

    private final Item item1 = new Item("item1","item1curie",Publisher.BBC);
    private final Item item2 = new Item("item2", "item2curie",Publisher.C4);
    
    private final Brand brand= new Brand("brand1", "brand2curie", Publisher.BBC);
    
    private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    private NewLookupWriter lookupStore = new NewLookupWriter() {
        @Override
        public void ensureLookup(Described described) {
        }
    };
    
    private final MongoContentTables contentTables = new MongoContentTables(mongo);
    private final MongoContentWriter writer = new MongoContentWriter(contentTables, lookupStore , new SystemClock());
    
    private final MongoContentLister lister = new MongoContentLister(contentTables);
   
    @Test
    public void testListContent() {
        
        writer.createOrUpdate(brand);
        writer.createOrUpdate(item1);
        writer.createOrUpdate(item2);
        
        final List<Content> contents = Lists.newArrayList();
        
        ContentListingHandler handler = new ContentListingHandler() {
            
            
            @Override
            public void handle(Content content, ContentListingProgress progress) {
                contents.add(content);
            }
        };
        
        
        lister.listContent(ImmutableSet.of(ContentTable.TOP_LEVEL_ITEMS), ContentListingProgress.START, handler);
        
        assertEquals(ImmutableList.of(item1, item2), contents);
        
    }

}
