package org.atlasapi.media.channel;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CachingChannelStoreTest {

    private static final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    private static final ChannelGroupStore channelGroupStore = new MongoChannelGroupStore(mongo);
    private static final MongoChannelStore store = new MongoChannelStore(
            mongo, channelGroupStore, channelGroupStore
    );

    private CachingChannelStore channelStore;

    @Before
    public void setUp() {
        DBCollection channels = mongo.collection("channels");
        BasicDBObject channel1 = new BasicDBObject("key", "channel1");
        BasicDBObject channel2 = new BasicDBObject("key", "channel2");
        channels.insert(channel1, channel2);

        channelStore = new CachingChannelStore(store);
    }

    @Test
    public void resolveChannelFromKey() {
        Maybe<Channel> channel2 = channelStore.fromKey("channel2");

        assertTrue(channel2.hasValue());
        assertEquals(channel2.requireValue().getKey(), "channel2");
    }

    @Test
    public void avoidNpeWhenResolveChannelFromNullKey() {
        Maybe<Channel> channel3 = channelStore.fromKey("channel3");

        assertFalse(channel3.hasValue());
        assertEquals(channel3.requireValue().getKey(), null);
    }
}