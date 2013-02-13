package org.atlasapi.media.channel;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Duration;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

public class MongoChannelStoreRetrievalTest {

    private static final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    
    private static final ChannelGroupStore channelGroupStore = new MongoChannelGroupStore(mongo);
    
    private static final MongoChannelStore store = new MongoChannelStore(mongo, channelGroupStore, channelGroupStore);
    
    private static Long channelId1;
    private static Long channelId2;
    private static Long channelId3;
    
    @BeforeClass
    public static void setUp() throws InterruptedException {
        channelId1 = store.createOrUpdate(channel("uri1", "key1", "test/1","test/2")).getId();
        channelId2 = store.createOrUpdate(channel("uri2", "key2", "asdf/1")).getId();
        channelId3 = store.createOrUpdate(channel("uri3", "key3", "test/3","asdf/2")).getId();
        Thread.sleep(2000);
    }
    
    private static Channel channel(String uri, String key, String... alias) {
        Channel channel = new Channel();
        channel.setCanonicalUri(uri);
        channel.setAliases(ImmutableSet.copyOf(alias));
        channel.setSource(Publisher.BBC);
        channel.setMediaType(MediaType.VIDEO);
        channel.setKey(key);
        channel.setAvailableFrom(ImmutableSet.<Publisher>of());
        return channel;
    }

    @Test
    public void testRetrievesAChannel() {
        
        Maybe<Channel> channel = store.fromId(channelId1);
        
        assertTrue(channel.hasValue());
        assertThat(channel.requireValue().getCanonicalUri(), is(equalTo("uri1")));
        
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
        
        assertThat(Iterables.size(channels), is(3));
        
        Map<String,Channel> channelMap = Maps.uniqueIndex(channels, Identified.TO_URI);
        assertThat(channelMap.get("uri1").getId(), is(channelId1));
        assertThat(channelMap.get("uri2").getId(), is(channelId2));
        assertThat(channelMap.get("uri3").getId(), is(channelId3));
        
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
    public void testRetrievesChannelByURI() {
    	assertThat(store.fromUri("uri1").requireValue().getId(), is(channelId1));
    }
    
    @Test 
    public void testRetrievesChannelByKey() {
    	assertThat(store.fromKey("key1").requireValue().getId(), is(channelId1));
    }
}
