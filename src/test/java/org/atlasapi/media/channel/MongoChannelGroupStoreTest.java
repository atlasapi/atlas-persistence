package org.atlasapi.media.channel;

import org.atlasapi.persistence.media.channel.MongoChannelGroupStore;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertTrue;

import org.atlasapi.media.entity.Publisher;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

public class MongoChannelGroupStoreTest {


    private static final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    
    private static final MongoChannelGroupStore store = new MongoChannelGroupStore(mongo);

    private static final ImmutableList<Long> ids = ImmutableList.of(1234L, 1235L, 1236L);
    private static final long channelId = 9L;
    
    @BeforeClass
    public static void setup() {
        for (Long id : ids) {
            store.store(channelGroupWithId(id));
        }
        ChannelGroup group = channelGroupWithId(1235L);
        group.addChannel(channelId);
        store.store(group);
    }

    private static ChannelGroup channelGroupWithId(long id) {
        ChannelGroup channelGroup = new Region();
        channelGroup.setId(id);
        channelGroup.setAvailableCountries(ImmutableSet.of(Countries.GB));
        channelGroup.addTitle("ChannelGroup" + id);
        channelGroup.setPublisher(Publisher.METABROADCAST);
        return channelGroup;
    }
    
    @Test
    public void testStoresAndRetrievesChannelGroup() {
        long id = 1234L;
        Optional<ChannelGroup> group = store.channelGroupFor(id);
        
        assertTrue(group.isPresent());
        assertThat(group.get().getId(), is(id));
        
    }
    
    @Test
    public void testRetrievesGroupsForIds() {
        ImmutableList<Long> requestedIds = ImmutableList.of(1234L, 1235L);
        Iterable<ChannelGroup> groups = store.channelGroupsFor(requestedIds);
        
        assertThat(Iterables.size(groups), is(2));
        assertThat(Iterables.get(groups, 0).getId(), isIn(requestedIds));
        assertThat(Iterables.get(groups, 1).getId(), isIn(requestedIds));
        
    }
    
    @Test
    public void testRetrievesAllGroups() {
        Iterable<ChannelGroup> groups = store.channelGroups();
        
        assertThat(Iterables.size(groups), is(3));
        assertThat(Iterables.get(groups, 0).getId(), isIn(ids));
        assertThat(Iterables.get(groups, 1).getId(), isIn(ids));
        assertThat(Iterables.get(groups, 2).getId(), isIn(ids));
    }
    
    @Test
    public void testRetrieveGroupForChannel() {
        Channel channel = new Channel();
        channel.setId(channelId);
        Iterable<ChannelGroup> groups = store.channelGroupsFor(channel);
        
        assertThat(Iterables.size(groups), is(1));
        assertThat(Iterables.get(groups, 0).getId(), is(1235L));
    }
}