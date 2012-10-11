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
import org.atlasapi.media.entity.Brand;
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
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.time.DateTimeZones;

@RunWith(MockitoJUnitRunner.class)
public class EsScheduleIndexTest {
    
    private Node esClient;
    private EsScheduleIndex scheduleIndex;
    private ESContentIndexer contentIndexer = new ESContentIndexer(esClient);

    private final Channel channel1 = new Channel(Publisher.METABROADCAST,"MB1","MB1",false,MediaType.VIDEO, "http://www.bbc.co.uk/services/bbcone");
    private final Channel channel2 = new Channel(Publisher.METABROADCAST,"MB1","MB1",false,MediaType.VIDEO, "http://www.bbc.co.uk/services/bbctwo");
    
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
        
        Item contained = itemWithBroadcast("contained", channel1.getCanonicalUri(), new DateTime(10, UTC), new DateTime(20, UTC));
        
        contentIndexer.index(contained);
        Thread.sleep(1000);

        Interval interval = new Interval(new DateTime(00, UTC), new DateTime(30, UTC));
        Future<ScheduleRef> futureEntries = scheduleIndex.resolveSchedule(METABROADCAST, channel1, interval);
        
        ScheduleRef scheduleRef  = futureEntries.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(1));
        assertThat(entries.get(0).getItemUri(), is(contained.getCanonicalUri()));
        
    }
    
    @Test
    public void testReturnsContentOverlappingInterval() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);

        Item overlapStart = itemWithBroadcast("overlapStart", channel1.getCanonicalUri(), start, start.plusHours(1));
        Item overlapEnd = itemWithBroadcast("overlapEnd", channel1.getCanonicalUri(), start.plusHours(2), start.plusHours(3));
        
        contentIndexer.index(overlapEnd);
        contentIndexer.index(overlapStart);
        
        Thread.sleep(1000);

        ListenableFuture<ScheduleRef> futureEntries = scheduleIndex.resolveSchedule(METABROADCAST, channel1, new Interval(start.plusMinutes(30), start.plusMinutes(150)));
        
        ScheduleRef scheduleRef  = futureEntries.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(2));
        assertThat(entries.get(0).getItemUri(), is(overlapStart.getCanonicalUri()));
        assertThat(entries.get(1).getItemUri(), is(overlapEnd.getCanonicalUri()));
    }
    
    @Test
    public void testReturnsContentContainingInterval() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);

        Item containsInterval = itemWithBroadcast("contains", channel1.getCanonicalUri(), start, start.plusHours(3));
        
        contentIndexer.index(containsInterval);
        
        Thread.sleep(1000);
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, new Interval(start.plusMinutes(30), start.plusMinutes(150)));

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
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, new Interval(start.plusMinutes(30), start.plusMinutes(150)));
        
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(0));
    }
    
    @Test
    public void testDoesntReturnContentOutsideInterval() throws Exception {
        DateTime start = new DateTime(DateTimeZones.UTC);

        Item tooLate = itemWithBroadcast("late", channel1.getCanonicalUri(), start.plusHours(3), start.plusHours(4));
      
        contentIndexer.index(tooLate);
        Thread.sleep(1000);
          
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, new Interval(start.plusMinutes(30), start.plusMinutes(150)));
          
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
          
        assertThat(entries.size(), is(0));
    }
    
    @Test
    public void testReturnsContentMatchingIntervalExactly() throws Exception {
        
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        
        Item exactMatch = itemWithBroadcast("exact", channel1.getCanonicalUri(), interval.getStart(), interval.getEnd());
        
        contentIndexer.index(exactMatch);
        Thread.sleep(1000);
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, interval);
        
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        ImmutableList<ScheduleRefEntry> entries = scheduleRef.getScheduleEntries();
        
        assertThat(entries.size(), is(1));
        assertThat(entries.get(0).getItemUri(), is(exactMatch.getCanonicalUri()));
        
    }
    
    @Test
    public void testOnlyReturnsExactlyMatchingScheduleRef() throws Exception {
        
        Interval interval1 = new Interval(0, 100, DateTimeZones.UTC);
        Interval interval2 = new Interval(150, 200, DateTimeZones.UTC);
        
        Item itemWith2Broadcasts = itemWithBroadcast("exact", channel1.getCanonicalUri(), interval1.getStart(), interval1.getEnd());
        Broadcast broadcast = new Broadcast(channel2.getCanonicalUri(), interval2.getStart(), interval2.getEnd());
        Iterables.getOnlyElement(itemWith2Broadcasts.getVersions()).addBroadcast(broadcast);
        
        contentIndexer.index(itemWith2Broadcasts);
        Thread.sleep(1000);
        
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, interval2);
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        
        assertThat(scheduleRef.getScheduleEntries().size(), is(0));

        futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel2, interval1);
        scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);

        assertThat(scheduleRef.getScheduleEntries().size(), is(0));

        futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, interval1);
        scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        
        assertThat(scheduleRef.getScheduleEntries().size(), is(1));
        
        futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel2, interval2);
        scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        
        assertThat(scheduleRef.getScheduleEntries().size(), is(1));
    }
    
    @Test
    public void testThatItemAppearingTwiceInScheduleGetsTwoEntries() throws Exception {
        
        Interval interval1 = new Interval(0, 100, DateTimeZones.UTC);
        Interval interval2 = new Interval(150, 200, DateTimeZones.UTC);
        
        Item itemWith2Broadcasts = itemWithBroadcast("exact", channel1.getCanonicalUri(), interval1.getStart(), interval1.getEnd());
        Broadcast broadcast = new Broadcast(channel1.getCanonicalUri(), interval2.getStart(), interval2.getEnd());
        Iterables.getOnlyElement(itemWith2Broadcasts.getVersions()).addBroadcast(broadcast);
        
        contentIndexer.index(itemWith2Broadcasts);
        Thread.sleep(1000);
        
        Interval queryInterval = new Interval(interval1.getStartMillis(), interval2.getEndMillis(), DateTimeZones.UTC);
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, queryInterval);
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        
        assertThat(scheduleRef.getScheduleEntries().size(), is(2));
        
    }
    
    @Test
    public void testFindsBothChildrenAndTopLevelItems() throws Exception {
        
        Interval interval1 = new Interval(0, 100, DateTimeZones.UTC);
        Interval interval2 = new Interval(150, 200, DateTimeZones.UTC);
        
        Item childItem = itemWithBroadcast("exact", channel1.getCanonicalUri(), interval1.getStart(), interval1.getEnd());
        childItem.setContainer(new Brand("brandUri","brandCurie",METABROADCAST));
        
        Item topItem = itemWithBroadcast("exact", channel1.getCanonicalUri(), interval2.getStart(), interval2.getEnd());
        
        contentIndexer.index(childItem);
        contentIndexer.index(topItem);
        Thread.sleep(1000);
        
        Interval queryInterval = new Interval(interval1.getStartMillis(), interval2.getEndMillis(), DateTimeZones.UTC);
        ListenableFuture<ScheduleRef> futureRef = scheduleIndex.resolveSchedule(METABROADCAST, channel1, queryInterval);
        ScheduleRef scheduleRef  = futureRef.get(5, TimeUnit.SECONDS);
        
        assertThat(scheduleRef.getScheduleEntries().size(), is(2));
        
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
