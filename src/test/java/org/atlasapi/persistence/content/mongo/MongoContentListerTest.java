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
import static org.mockito.Mockito.mock;

import java.util.Arrays;
import java.util.List;

import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.query.Selection;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.EventRef;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.audit.PerHourAndDayMongoPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.lookup.mongo.MongoLookupEntryStore;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.ReadPreference;

@RunWith( MockitoJUnitRunner.class )
public class MongoContentListerTest {

    private static final Item bbcItem1 = new Item("bbcItem1","bbcItem1curie",Publisher.BBC);
    private static final Item bbcItem2 = new Item("bbcItem2", "bbcItem2curie",Publisher.BBC);
    private static final Item c4Item1 = new Item("aC4Item1", "c4Item1curie",Publisher.C4);
    private static final Item c4Item2 = new Item("aC4Item2", "c4Item2curie",Publisher.C4);
    private static final Brand bbcBrand= new Brand("bbcBrand1", "bbcBrand1curie", Publisher.BBC);
    private static final Brand c4Brand= new Brand("c4Brand1", "c4Brand1curie", Publisher.C4);

    private static final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    private static final PersistenceAuditLog persistenceAuditLog = new PerHourAndDayMongoPersistenceAuditLog(mongo);
    
    private final MongoContentLister lister = new MongoContentLister(mongo, 
            new MongoContentResolver(mongo, new MongoLookupEntryStore(
                    mongo.collection("lookup"), persistenceAuditLog, ReadPreference.primary())));

    private static final ServiceResolver serviceResolver = mock(ServiceResolver.class);
    private static final PlayerResolver playerResolver = mock(PlayerResolver.class);
    private static final ContentQuery matchEverythingContentQuery = new ContentQuery(ImmutableList.of(), Selection.ALL, mock(Application.class));
    
    private static final DateTime tenthOfTheTenth = new DateTime(2011,10,10,00,00,00,000,DateTimeZones.UTC);
    private static final DateTime ELEVENTH_OF_THE_TENTH = new DateTime(2011,10,11,00,00,00,000,DateTimeZones.UTC);
    
    @BeforeClass
    public static void writeTestContents() {

        NewLookupWriter lookupStore = new NewLookupWriter() {
            @Override
            public void ensureLookup(Content described) {
            }
        };
        
        bbcItem1.setLastUpdated(tenthOfTheTenth);
        bbcItem2.setLastUpdated(ELEVENTH_OF_THE_TENTH);
        c4Item1.setLastUpdated(ELEVENTH_OF_THE_TENTH);
        c4Item2.setLastUpdated(tenthOfTheTenth);
        bbcBrand.setLastUpdated(tenthOfTheTenth);
        c4Brand.setLastUpdated(ELEVENTH_OF_THE_TENTH);
        
        TopicRef topic1 = new TopicRef(1l, 0.01f, true, TopicRef.Relationship.ABOUT);
        TopicRef topic2 = new TopicRef(2l, 0.01f, true, TopicRef.Relationship.ABOUT);
        TopicRef topic3 = new TopicRef(3l, 0.01f, true, TopicRef.Relationship.ABOUT);

        bbcItem1.setTopicRefs(ImmutableSet.of(topic1, topic2, topic3));
        c4Item1.setTopicRefs(ImmutableSet.of(topic2, topic3));
        c4Item2.setTopicRefs(ImmutableSet.of(topic1, topic3));
        bbcBrand.setTopicRefs(ImmutableSet.of(topic1, topic2));
        c4Brand.setTopicRefs(ImmutableSet.of(topic1, topic3));

        EventRef eventRef1 = new EventRef(1234L);
        eventRef1.setPublisher(Publisher.BBC);

        EventRef eventRef2 = new EventRef(12345L);
        eventRef2.setPublisher(Publisher.BBC);

        bbcItem1.setEventRefs(ImmutableList.<EventRef>of(eventRef1));
        bbcItem2.setEventRefs(ImmutableList.<EventRef>of(eventRef2));

        MongoContentWriter writer = new MongoContentWriter(mongo, lookupStore, persistenceAuditLog, playerResolver, serviceResolver, 
                new SystemClock());
        writer.createOrUpdate(bbcBrand);
        writer.createOrUpdate(c4Brand);
        writer.createOrUpdate(bbcItem1);
        writer.createOrUpdate(bbcItem2);
        writer.createOrUpdate(c4Item1);
        writer.createOrUpdate(c4Item2);

    }
    
    @Test
    public void testTopicListing() {
        
        ImmutableSet<Content> contents = ImmutableSet.copyOf(lister.contentForTopic(1l, matchEverythingContentQuery));
        assertThat(contents, is(equalTo(ImmutableSet.<Content>of(bbcItem1,c4Item2,bbcBrand,c4Brand))));
        
        
        contents = ImmutableSet.copyOf(lister.contentForTopic(2l, matchEverythingContentQuery));
        assertThat(contents, is(equalTo(ImmutableSet.<Content>of(bbcItem1,c4Item1,bbcBrand))));
        
        
        contents = ImmutableSet.copyOf(lister.contentForTopic(3l, matchEverythingContentQuery));
        assertThat(contents, is(equalTo(ImmutableSet.<Content>of(bbcItem1,c4Item1,c4Item2,c4Brand))));
        
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
    
    @Test
    public void testLastUpdatedListing() {
        ImmutableList<Content> contents = ImmutableList.copyOf(lister.updatedSince(BBC, tenthOfTheTenth.minusDays(1)));
        assertThat(contents, is(equalTo(ImmutableList.of(bbcBrand, bbcItem1, bbcItem2))));

        contents = ImmutableList.copyOf(lister.updatedSince(BBC, tenthOfTheTenth.plusHours(6)));
        assertThat(contents, is(equalTo(ImmutableList.<Content>of(bbcItem2))));

        contents = ImmutableList.copyOf(lister.updatedSince(BBC, ELEVENTH_OF_THE_TENTH.plusHours(6)));
        assertThat(contents, is(equalTo(ImmutableList.<Content>of())));

        contents = ImmutableList.copyOf(lister.updatedSince(C4, tenthOfTheTenth.minusDays(1)));
        assertThat(contents, is(equalTo(ImmutableList.of(c4Brand, c4Item2, c4Item1))));
        
        contents = ImmutableList.copyOf(lister.updatedSince(C4, tenthOfTheTenth.plusHours(6)));
        assertThat(contents, is(equalTo(ImmutableList.<Content>of(c4Brand, c4Item1))));
        
        contents = ImmutableList.copyOf(lister.updatedSince(C4, ELEVENTH_OF_THE_TENTH.plusHours(6)));
        assertThat(contents, is(equalTo(ImmutableList.<Content>of())));
    }

    @Test
    public void testContentForEventIds() {
        List<Content> contents = ImmutableList.copyOf(lister.contentForEvent(
                Arrays.asList(1234L, 12345L), matchEverythingContentQuery));
        assertEquals(contents.get(0).getCanonicalUri(), "bbcItem1" );
        assertEquals(contents.get(1).getCanonicalUri(), "bbcItem2");

    }
}
