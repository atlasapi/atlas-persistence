package org.atlasapi.media.channel;

import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CachingChannelStoreTest {

    private CachingChannelStore cachingChannelStore;

    @Before
    public void setUp() {
        DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
        ChannelGroupStore channelGroupStore = new MongoChannelGroupStore(mongo);
        MongoChannelStore mongoChannelStore = new MongoChannelStore(
                mongo,
                channelGroupStore,
                channelGroupStore
        );

        Channel channelWithKey = createChannel("key", "channel", null, "alias");
        mongoChannelStore.createOrUpdate(channelWithKey);
        Channel channelWithNullKey = createChannel("nullKey", null, null, "alias");
        mongoChannelStore.createOrUpdate(channelWithNullKey);

        cachingChannelStore = new CachingChannelStore(mongoChannelStore);
    }

    private Channel createChannel(String uri, String key, Long parent, String... alias) {
        Channel channel = Channel.builder()
                .withUri(uri)
                .withKey(key)
                .withSource(Publisher.PA)
                .withMediaType(MediaType.VIDEO)
                .withParent(parent)
                .build();
        channel.setAliasUrls(ImmutableSet.copyOf(alias));
        return channel;
    }

    @Test
    public void resolveChannelFromKey() {
        Maybe<Channel> channel = cachingChannelStore.fromKey("channel");

        assertTrue(channel.hasValue());
        assertEquals(channel.requireValue().getKey(), "channel");
    }

    @Test
    public void avoidNpeWhenResolveChannelFromNullKey() {
        Maybe<Channel> channelWithNoKey = cachingChannelStore.fromKey("noKeyChannel");

        assertFalse(channelWithNoKey.hasValue());
        assertEquals(channelWithNoKey.requireValue().getKey(), null);
    }
}