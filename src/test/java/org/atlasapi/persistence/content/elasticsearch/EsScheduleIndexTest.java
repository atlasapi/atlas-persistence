package org.atlasapi.persistence.content.elasticsearch;

import static com.metabroadcast.common.time.DateTimeZones.UTC;
import static org.atlasapi.media.entity.Publisher.METABROADCAST;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.elasticsearch.schema.ESSchema;
import org.atlasapi.persistence.content.schedule.ScheduleRef;
import org.atlasapi.persistence.content.schedule.ScheduleRef.ScheduleRefEntry;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.time.DateTimeZones;

@RunWith(MockitoJUnitRunner.class)
public class EsScheduleIndexTest {
    
    private Node esClient;
    private EsScheduleIndex scheduleIndex;
    private ESContentIndexer contentIndexer = new ESContentIndexer(esClient);

    private final Channel channel = new Channel(Publisher.METABROADCAST,"MB","MB",MediaType.VIDEO, "http://www.bbc.co.uk/services/bbcone");
    
    @Before
    public void before() throws Exception {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
            new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
        esClient = NodeBuilder.nodeBuilder().local(true).clusterName(ESSchema.CLUSTER_NAME).build().start();
        contentIndexer = new ESContentIndexer(esClient);
        contentIndexer.init();
        scheduleIndex = new EsScheduleIndex(esClient);
    }
    
    @After
    public void after() throws Exception {
        esClient.client().admin().indices().delete(Requests.deleteIndexRequest(ESSchema.INDEX_NAME)).actionGet();
        esClient.close();
    }
    
    @Test
    public void testReturnsContentContainedInInterval() throws Exception {
        
        Item contained = itemWithBroadcast("contained", channel.getCanonicalUri(), new DateTime(10, UTC), new DateTime(20, UTC));

        contentIndexer.index(contained);
        Thread.sleep(1000);

        Interval interval = new Interval(new DateTime(00, UTC), new DateTime(30, UTC));
        Future<ScheduleRef> futureEntries = scheduleIndex.resolveSchedule(METABROADCAST, channel, interval);
        
        ScheduleRef scheduleRef  = futureEntries.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(1));
        assertThat(entries.get(0).getItemUri(), is(contained.getCanonicalUri()));
        
    }
    
    @Test
    public void testReturnsContentOverlappingInterval() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);

        Item overlapStart = itemWithBroadcast("overlapStart", channel.getCanonicalUri(), start, start.plusHours(1));
        Item overlapEnd = itemWithBroadcast("overlapEnd", channel.getCanonicalUri(), start.plusHours(2), start.plusHours(3));
        
        contentIndexer.index(overlapEnd);
        contentIndexer.index(overlapStart);
        
        Thread.sleep(1000);

        ListenableFuture<ScheduleRef> futureEntries = scheduleIndex.resolveSchedule(METABROADCAST, channel, new Interval(start.plusMinutes(30), start.plusMinutes(150)));
        
        ScheduleRef scheduleRef  = futureEntries.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(2));
        assertThat(entries.get(0).getItemUri(), is(overlapStart.getCanonicalUri()));
        assertThat(entries.get(1).getItemUri(), is(overlapEnd.getCanonicalUri()));
    }
    
    @Test
    public void testReturnsContentContainingInterval() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);

        Item containsInterval = itemWithBroadcast("contains", channel.getCanonicalUri(), start, start.plusHours(3));
        
        contentIndexer.index(containsInterval);
        
        Thread.sleep(1000);
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel, new Interval(start.plusMinutes(30), start.plusMinutes(150)));

        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(1));
        assertThat(entries.get(0).getItemUri(), is(containsInterval.getCanonicalUri()));
    }

    @Test
    public void testDoesntReturnContentOnDifferentChannel() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);

        Item wrongChannel = itemWithBroadcast("wrong", "http://www.bbc.co.uk/services/bbctwo", start.plusHours(1), start.plusHours(2));
        
        contentIndexer.index(wrongChannel);
        
        Thread.sleep(1000);
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel, new Interval(start.plusMinutes(30), start.plusMinutes(150)));
        
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(0));
    }
    
    @Test
    public void testDoesntReturnContentOutsideInterval() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);

        Item tooLate = itemWithBroadcast("late", channel.getCanonicalUri(), start.plusHours(3), start.plusHours(4));
      
        contentIndexer.index(tooLate);
        Thread.sleep(1000);
          
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel, new Interval(start.plusMinutes(30), start.plusMinutes(150)));
          
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
          
        assertThat(entries.size(), is(0));
    }
    
    @Test
    public void testReturnsContentMatchingIntervalExactly() throws Exception {
        
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        
        Item exactMatch = itemWithBroadcast("exact", channel.getCanonicalUri(), interval.getStart(), interval.getEnd());
        
        contentIndexer.index(exactMatch);
        Thread.sleep(1000);
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel, interval);
        
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(1));
        assertThat(entries.get(0).getItemUri(), is(exactMatch.getCanonicalUri()));
        
    }
    
    private Item itemWithBroadcast(String itemUri, String channelUri, DateTime start, DateTime end) {
        
        Broadcast broadcast = new Broadcast(channelUri, start, end);
        Version version = new Version();
        Item item = new Item(itemUri, itemUri, Publisher.METABROADCAST);
        version.addBroadcast(broadcast);
        item.addVersion(version);
        
        return item;
    }
    
}
