package org.atlasapi.persistence.content.mongo;

import static org.junit.Assert.assertEquals;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Channel;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.media.entity.Version;
import org.atlasapi.media.entity.Schedule.ScheduleChannel;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.time.DateTimeZones;


public class MongoScheduleStoreTest {

    private MongoScheduleStore store;
    
    private final DateTime now = new DateTime(DateTimeZones.UTC);
    private final Broadcast broadcast1 = new Broadcast(Channel.BBC_ONE.uri(), now.minusHours(4), now.minusHours(2));
    private final Broadcast broadcast2 = new Broadcast(Channel.BBC_TWO.uri(), now.minusHours(4), now.minusHours(1));
    private final Broadcast broadcast3 = new Broadcast(Channel.BBC_ONE.uri(), now.minusHours(2), now.minusHours(1));
    private final Broadcast broadcast4 = new Broadcast(Channel.BBC_TWO.uri(), now.minusHours(1), now);
    
    private final Version version1 = new Version();
    private final Version version2 = new Version();
    private final Version version3 = new Version();
    
    private final Item item1 = new Item("item1", "item1", Publisher.BBC);
    private final Item item2 = new Item("item2", "item2", Publisher.BBC);
    
    @Before
    public void setUp() throws Exception {
        store = new MongoScheduleStore(MongoTestHelper.anEmptyTestDatabase());
        
        version1.addBroadcast(broadcast1);
        version2.addBroadcast(broadcast2);
        version3.addBroadcast(broadcast3);
        version3.addBroadcast(broadcast4);
        
        item1.addVersion(version1);
        item2.addVersion(version2);
        item2.addVersion(version3);
    }
    
    @Test
    public void shouldSaveItemsAndRetrieveSchedule() throws Exception {
        store.createOrUpdate(item1);
        store.createOrUpdate(item2);
        
        Schedule schedule = store.schedule(now.minusHours(4), now, ImmutableSet.of(Channel.BBC_ONE, Channel.BBC_TWO), ImmutableSet.of(Publisher.BBC));
        
        assertEquals(2, schedule.scheduleChannels().size());
        
        for (ScheduleChannel channel: schedule.scheduleChannels()) {
            assertEquals(2, channel.items().size());
            
            Item item1 = channel.items().get(0);
            Broadcast broadcast1 = ScheduleEntry.BROADCAST.apply(item1);
            
            Item item2 = channel.items().get(1);
            Broadcast broadcast2 = ScheduleEntry.BROADCAST.apply(item2);
            
            if (channel.channel() == Channel.BBC_ONE) {
                assertEquals(now.minusHours(4), broadcast1.getTransmissionTime());
                assertEquals(now.minusHours(2), broadcast2.getTransmissionTime());
            } else {
                assertEquals(now.minusHours(4), broadcast1.getTransmissionTime());
                assertEquals(now.minusHours(1), broadcast2.getTransmissionTime());
            }
        }
    }
}
