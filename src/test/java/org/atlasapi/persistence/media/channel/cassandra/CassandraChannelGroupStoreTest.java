package org.atlasapi.persistence.media.channel.cassandra;

import com.google.common.collect.Iterables;
import java.util.Arrays;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.cassandra.BaseCassandraTest;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

//@Ignore(value = "Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraChannelGroupStoreTest extends BaseCassandraTest {

    private CassandraChannelGroupStore store;

    @Before
    @Override
    public void before() {
        super.before();
        store = new CassandraChannelGroupStore(context, 10000);
    }

    @Test
    public void testChannelGroupById() {
        ChannelGroup group1 = new ChannelGroup();
        group1.setId(1l);
        group1.setCanonicalUri("g1");
        group1.setChannels(Arrays.asList(1l));

        store.store(group1);

        assertEquals(group1, store.channelGroupFor(1l).get());
    }

    @Test
    public void testChannelGroupByIds() {
        ChannelGroup group1 = new ChannelGroup();
        group1.setId(1l);
        group1.setCanonicalUri("g1");
        group1.setChannels(Arrays.asList(1l));
        ChannelGroup group2 = new ChannelGroup();
        group2.setId(2l);
        group2.setCanonicalUri("g2");
        group2.setChannels(Arrays.asList(1l));

        store.store(group1);
        store.store(group2);

        assertEquals(2, Iterables.size(store.channelGroupsFor(Arrays.asList(1l, 2l))));
    }

    @Test
    public void testChannelGroupByChannel() {
        ChannelGroup group1 = new ChannelGroup();
        group1.setId(1l);
        group1.setCanonicalUri("g1");
        group1.setChannels(Arrays.asList(1l));
        ChannelGroup group2 = new ChannelGroup();
        group2.setId(2l);
        group2.setCanonicalUri("g2");
        group2.setChannels(Arrays.asList(1l));

        store.store(group1);
        store.store(group2);

        Channel channel1 = new Channel(Publisher.METABROADCAST, "channel1", "k1", MediaType.AUDIO, "channel1");
        channel1.setId(1l);

        assertEquals(2, Iterables.size(store.channelGroupsFor(channel1)));
    }
    
    @Test
    public void testChangeChannelsBetweenGroups() {
        ChannelGroup group1 = new ChannelGroup();
        group1.setId(1l);
        group1.setCanonicalUri("g1");
        group1.setChannels(Arrays.asList(1l, 2l, 3l));
        ChannelGroup group2 = new ChannelGroup();
        group2.setId(2l);
        group2.setCanonicalUri("g2");
        group2.setChannels(Arrays.asList(1l, 2l, 3l));

        store.store(group1);
        store.store(group2);

        Channel channel1 = new Channel(Publisher.METABROADCAST, "channel1", "k1", MediaType.AUDIO, "channel1");
        channel1.setId(1l);
        Channel channel2 = new Channel(Publisher.METABROADCAST, "channel2", "k2", MediaType.AUDIO, "channel2");
        channel2.setId(2l);
        Channel channel3 = new Channel(Publisher.METABROADCAST, "channel3", "k3", MediaType.AUDIO, "channel3");
        channel3.setId(3l);

        assertEquals(2, Iterables.size(store.channelGroupsFor(channel1)));
        assertEquals(2, Iterables.size(store.channelGroupsFor(channel2)));
        assertEquals(2, Iterables.size(store.channelGroupsFor(channel3)));

        group1.setChannels(Arrays.asList(1l));
        group2.setChannels(Arrays.asList(2l));

        store.store(group1);
        store.store(group2);

        assertEquals(1, Iterables.size(store.channelGroupsFor(channel1)));
        assertEquals(group1, Iterables.get(store.channelGroupsFor(channel1), 0));
        assertEquals(1, Iterables.size(store.channelGroupsFor(channel2)));
        assertEquals(group2, Iterables.get(store.channelGroupsFor(channel2), 0));
        assertEquals(0, Iterables.size(store.channelGroupsFor(channel3)));
    }
}
