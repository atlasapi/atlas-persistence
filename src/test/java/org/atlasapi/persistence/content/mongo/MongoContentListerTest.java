package org.atlasapi.persistence.content.mongo;

import static org.atlasapi.media.entity.Publisher.BBC;
import static org.atlasapi.media.entity.Publisher.C4;
import static org.atlasapi.persistence.content.ContentCategory.CONTAINER;
import static org.atlasapi.persistence.content.ContentCategory.TOP_LEVEL_ITEM;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.BasicDBObject;

public class MongoContentListerTest {

    private static final Item bbcItem1 = new Item("bbcItem1","bbcItem1curie",Publisher.BBC);
    private static final Item bbcItem2 = new Item("bbcItem2", "bbcItem2curie",Publisher.BBC);
    private static final Item c4Item1 = new Item("aC4Item1", "c4Item1curie",Publisher.C4);
    private static final Item c4Item2 = new Item("aC4Item2", "c4Item2curie",Publisher.C4);
    private static final Brand bbcBrand= new Brand("bbcBrand1", "bbcBrand1curie", Publisher.BBC);
    private static final Brand c4Brand= new Brand("c4Brand1", "c4Brand1curie", Publisher.C4);
    
    private static final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    
    private final MongoContentLister lister = new MongoContentLister(mongo);
    
    @BeforeClass
    public static void writeTestContents() {
        mongo.collection(TOP_LEVEL_ITEM.tableName()).ensureIndex(new BasicDBObject(ImmutableMap.of("publisher",1,MongoConstants.ID,1)));
        mongo.collection(CONTAINER.tableName()).ensureIndex(new BasicDBObject(ImmutableMap.of("publisher",1,MongoConstants.ID,1)));
        NewLookupWriter lookupStore = new NewLookupWriter() {
            @Override
            public void ensureLookup(Described described) {
            }
        };
        MongoContentWriter writer = new MongoContentWriter(mongo, lookupStore, new SystemClock());
        writer.createOrUpdate(bbcBrand);
        writer.createOrUpdate(c4Brand);
        writer.createOrUpdate(bbcItem1);
        writer.createOrUpdate(bbcItem2);
        writer.createOrUpdate(c4Item1);
        writer.createOrUpdate(c4Item2);
    }
    
    @Test
    public void testListContentFromOneTable() {
        
        List<Content> contents = ImmutableList.copyOf(lister.listContent(defaultCriteria().forContent(TOP_LEVEL_ITEM).forPublishers(ImmutableList.of(BBC,C4)).build()));
        
        assertEquals(ImmutableList.of(bbcItem1, bbcItem2, c4Item1, c4Item2), contents);
        
    }
    
    @Test
    public void testListContentFromTwoTables() {
        
        List<Content> contents = ImmutableList.copyOf(lister.listContent(defaultCriteria().forContent(TOP_LEVEL_ITEM, CONTAINER).forPublishers(BBC,C4).build()));
        
        assertEquals(ImmutableList.of(bbcItem1, bbcItem2, bbcBrand, c4Item1, c4Item2, c4Brand), contents);
        
    }

    @Test
    public void testRestartListing() {
        
        ContentListingProgress progress = ContentListingProgress.START;
        ImmutableList<Content> contents = ImmutableList.copyOf(lister.listContent(defaultCriteria().forContent(TOP_LEVEL_ITEM, CONTAINER).forPublishers(BBC,C4).startingAt(progress).build()));
        assertThat(contents, is((equalTo(ImmutableList.of(bbcItem1, bbcItem2, bbcBrand, c4Item1, c4Item2, c4Brand)))));

        progress = ContentListingProgress.progressFrom(bbcItem1);
        contents = ImmutableList.copyOf(lister.listContent(defaultCriteria().forContent(TOP_LEVEL_ITEM, CONTAINER).forPublishers(BBC,C4).startingAt(progress).build()));
        assertThat(contents, is((equalTo(ImmutableList.of(bbcItem2, bbcBrand, c4Item1, c4Item2, c4Brand)))));

        progress = ContentListingProgress.progressFrom(bbcItem2);
        contents = ImmutableList.copyOf(lister.listContent(defaultCriteria().forContent(TOP_LEVEL_ITEM, CONTAINER).forPublishers(BBC,C4).startingAt(progress).build()));
        assertThat(contents, is((equalTo(ImmutableList.of(bbcBrand, c4Item1, c4Item2, c4Brand)))));

        progress = ContentListingProgress.progressFrom(bbcBrand);
        contents = ImmutableList.copyOf(lister.listContent(defaultCriteria().forContent(TOP_LEVEL_ITEM, CONTAINER).forPublishers(BBC,C4).startingAt(progress).build()));
        assertThat(contents, is((equalTo(ImmutableList.of(c4Item1, c4Item2, c4Brand)))));

        progress = ContentListingProgress.progressFrom(c4Item2);
        contents = ImmutableList.copyOf(lister.listContent(defaultCriteria().forContent(TOP_LEVEL_ITEM, CONTAINER).forPublishers(BBC,C4).startingAt(progress).build()));
        assertThat(contents, is((equalTo(ImmutableList.<Content>of(c4Brand)))));

        progress = ContentListingProgress.progressFrom(c4Brand);
        contents = ImmutableList.copyOf(lister.listContent(defaultCriteria().forContent(TOP_LEVEL_ITEM, CONTAINER).forPublishers(BBC,C4).startingAt(progress).build()));
        assertThat(contents, is((equalTo(ImmutableList.<Content>of()))));
        
    }
}
