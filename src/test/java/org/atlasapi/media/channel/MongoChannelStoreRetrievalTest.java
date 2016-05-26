package org.atlasapi.media.channel;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import com.google.common.base.Predicate;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import javax.annotation.Nullable;

public class MongoChannelStoreRetrievalTest {

    private static final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    
    private static final ChannelGroupStore channelGroupStore = new MongoChannelGroupStore(mongo);
    
    private static final MongoChannelStore store = new MongoChannelStore(mongo, channelGroupStore, channelGroupStore);
    
    private static Long channelId1;
    private static Long channelId2;
    private static Long channelId3;
    private static Long channelId4;
    private static Long channelId5;
    private static DateTime dateTime;
    @BeforeClass
    public static void setUp() throws InterruptedException {
        dateTime = DateTime.now();
        channelId1 = store.createOrUpdate(channel(1, "uri1", "key1", "sport", dateTime, "test/1","test/2")).getId();
        channelId2 = store.createOrUpdate(channel(2, "uri2", "key2", "not-sport", null, "asdf/1")).getId();
        channelId3 = store.createOrUpdate(channel(3, "uri3", "key3", "flim", null, "test/3","asdf/2")).getId();
        channelId4 = store.createOrUpdate(channel(4, "uri4", "key4", "old episode", dateTime.minusDays(1), "test")).getId();
        channelId5 = store.createOrUpdate(channel(5, "uri5", "key5", "episode", dateTime.plusDays(1), "test")).getId();

        Thread.sleep(2000);
    }
    
    private static Channel channel(long id, String uri, String key, String genre,
            DateTime advertisedFrom, String... aliasUrls) {
        Channel channel = new Channel();
        channel.setCanonicalUri(uri);
        channel.setAliasUrls(ImmutableSet.copyOf(aliasUrls));
        channel.setSource(Publisher.BBC);
        channel.setMediaType(MediaType.VIDEO);
        channel.setKey(key);
        channel.setAvailableFrom(ImmutableSet.<Publisher>of());
        channel.setGenres(ImmutableSet.of(genre));
        channel.setAdvertiseFrom(advertisedFrom);
        return channel;
    }

    @Test
    public void testRetrievesAChannel() {
        
        Maybe<Channel> channel = store.fromId(channelId1);
        
        assertTrue(channel.hasValue());
        assertThat(channel.requireValue().getCanonicalUri(), is(equalTo("uri1")));

        assertThat(channel.requireValue().getAdvertiseFrom().getMillis(), is(equalTo(dateTime.getMillis())));

    }

    @Test
    public void testRetrievesSomeChannels() {
        
        List<Long> ids = Lists.newArrayList(channelId1, channelId3);
        Iterable<Channel> channels = store.forIds(ids);
        
        assertThat(Iterables.size(channels), is(2));
        Map<String,Channel> channelMap = Maps.uniqueIndex(channels, Identified.TO_URI);
        assertThat(channelMap.get("uri1").getId(), is(channelId1));
        assertThat(channelMap.get("uri2"), is(nullValue()));
        assertThat(channelMap.get("uri3").getId(), is(channelId3));
        
    }
    
    @Test
    public void testRetrievesAllChannels() {
        
        Iterable<Channel> channels = store.all();
        
        assertThat(Iterables.size(channels), is(5));
        
        Map<String,Channel> channelMap = Maps.uniqueIndex(channels, Identified.TO_URI);
        assertThat(channelMap.get("uri1").getId(), is(channelId1));
        assertThat(channelMap.get("uri2").getId(), is(channelId2));
        assertThat(channelMap.get("uri3").getId(), is(channelId3));
        
    }
    
    @Test
    public void testRetrievesAllChannelsInOrder() {
        
        Iterable<Channel> channels = store.all();
        
        List<Long> channelIds = ImmutableList.copyOf(Iterables.transform(channels, new Function<Channel, Long>() {
            @Override
            public Long apply(Channel input) {
                return input.getId();
            }
        }));
        
        List<Long> expectedIds = Ordering.natural().immutableSortedCopy(Lists.newArrayList(channelId1, channelId2, channelId3, channelId4, channelId5));
        
        assertEquals(expectedIds, channelIds);
    }

    @Test
    public void testRetrievesChannelsByAliasPrefix() {
        String prefix = "test/";
        Map<String, Channel> aliases = store.forAliases(prefix);
        
        assertThat(aliases.size(), is(3));
        assertThat(aliases.get(prefix+1).getId(),is(channelId1));
        assertThat(aliases.get(prefix+2).getId(),is(channelId1));
        assertThat(aliases.get(prefix+3).getId(),is(channelId3));
    }

    @Test
    public void testRetrievesChannelsByAlias() {
        Maybe<Channel> channel = store.forAlias("test/1");
        
        assertThat(channel.requireValue().getId(), is(channelId1));
    }
    
    @Test 
    public void testRetrievesChannelByURI() {
    	assertThat(store.fromUri("uri1").requireValue().getId(), is(channelId1));
    }
    
    @Test 
    public void testRetrievesChannelByKey() {
    	assertThat(store.fromKey("key1").requireValue().getId(), is(channelId1));
    }
    
    @Test
    public void testRetrievesChannelByGenre() {
        ChannelQuery query = ChannelQuery.builder().withGenres(ImmutableSet.of("sport")).build();
        Channel retrieved = Iterables.getOnlyElement(store.allChannels(query));
        
        assertThat(retrieved.getId(), is(channelId1));
    }

    @Test
    public void testRetrievesOldEpisodes() {
        ChannelQuery query = ChannelQuery.builder().withAdvertisedOn(dateTime).build();
        Iterable<Channel> channels = store.allChannels(query);
        assertThat(Iterables.size(channels), is(2));
    }

    @Test
    public void testDoesNotRetrieveEpisodesThatAreNotAdvertisedYet() {
        ChannelQuery query = ChannelQuery.builder().withAdvertisedOn(dateTime.minusDays(2)).build();
        Iterable<Channel> retrieved = store.allChannels(query);
        assertTrue(Iterables.isEmpty(retrieved));
    }

    @Test
    public void testRetrievesChannelByPublisher() {
        ChannelQuery query = ChannelQuery.builder().withPublisher(Publisher.BBC).build();
        Iterable<Channel> channels = store.allChannels(query);
        assertFalse(Iterables.isEmpty(channels));
        Iterable<Channel> filtered = Iterables.filter(channels, new Predicate<Channel>() {
            @Override
            public boolean apply(@Nullable Channel channel) {
                return !channel.getSource().equals(Publisher.BBC);
            }
        });
        assertTrue(Iterables.isEmpty(filtered));
    }

    @Test
    public void testRetrievesChannelByUri() {
        ChannelQuery query = ChannelQuery.builder().withUri("uri1").build();
        Iterable<Channel> channels = store.allChannels(query);
        assertFalse(Iterables.isEmpty(channels));
        assertTrue(Iterables.getOnlyElement(channels).getUri().equals("uri1"));
    }
}
