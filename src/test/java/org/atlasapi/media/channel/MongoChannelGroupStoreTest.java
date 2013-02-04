package org.atlasapi.media.channel;

import org.atlasapi.persistence.media.channel.MongoChannelGroupStore;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isIn;
import static org.junit.Assert.assertTrue;

import org.atlasapi.media.entity.Publisher;
import org.joda.time.LocalDate;
import org.junit.BeforeClass;
import org.junit.Ignore;
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

    private static final ImmutableList<Long> regionIds = ImmutableList.of(1234L, 1235L, 1236L);
    private static final long channelId = 9L;
    private static final long platformId = 999L;
    private static final ImmutableList<Long> ids = ImmutableList.<Long>builder()
        .addAll(regionIds)
        .add(platformId)
        .build();
    
    @BeforeClass
    public static void setup() {
        store.store(createPlatform());
        for (Long id : regionIds) {
            store.store(channelGroupWithId(id));
        }
        Region region = channelGroupWithId(1235L);
        region.addChannel(channelId);
        store.store(region);
    }

    private static Platform createPlatform() {
        Platform platform = new Platform();
        platform.setId(platformId);
        return platform;
    }

    private static Region channelGroupWithId(long id) {
        Region region = new Region();
        region.setId(id);
        region.setAvailableCountries(ImmutableSet.of(Countries.GB));
        region.addTitle("ChannelGroup" + id);
        region.setPublisher(Publisher.METABROADCAST);
        region.setPlatform(platformId);
        return region;
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
        
        assertThat(Iterables.size(groups), is(4));
        assertThat(Iterables.get(groups, 0).getId(), isIn(ids));
        assertThat(Iterables.get(groups, 1).getId(), isIn(ids));
        assertThat(Iterables.get(groups, 2).getId(), isIn(ids));
        assertThat(Iterables.get(groups, 3).getId(), isIn(ids));
    }
    
    // TODO modify to work with channelNumberings
    @Ignore("Needs refactor")
    @Test
    public void dontTestRetrieveGroupForChannel() {
        Channel channel = new Channel();
        channel.setId(channelId);
        Iterable<ChannelGroup> groups = store.channelGroupsFor(channel);
        
        assertThat(Iterables.size(groups), is(1));
        assertThat(Iterables.get(groups, 0).getId(), is(1235L));
    }
    
    @Test
    public void testAddMultipleNumberings() {
        ChannelGroup group = new Platform();
        group.setId(5678L);
        
        group = store.store(group);
        
        ChannelNumbering numbering = ChannelNumbering.builder()
                .withStartDate(new LocalDate(2011, 1, 1))
                .withChannel(1234L)
                .withChannelGroup(5678L)
                .withChannelNumber("1")
                .build();
        
        group.addChannelNumbering(numbering);
        group = store.store(group);
        
        assertThat(group.getChannelNumberings().size(), is(1));
        
        group.addChannelNumbering(numbering);
        group = store.store(group);
        
        assertThat(group.getChannelNumberings().size(), is(1));
    }
}