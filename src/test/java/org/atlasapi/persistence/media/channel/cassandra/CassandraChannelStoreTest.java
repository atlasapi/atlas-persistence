package org.atlasapi.persistence.media.channel.cassandra;

import com.metabroadcast.common.ids.UUIDGenerator;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.cassandra.BaseCassandraTest;
import org.joda.time.Duration;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;

@Ignore(value = "Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraChannelStoreTest extends BaseCassandraTest {
    
    private CassandraChannelStore store;
    private Channel channel;
    
    @Before
    @Override
    public void before() {
        super.before();
        store = new CassandraChannelStore(context, 10000, new UUIDGenerator(), Duration.standardSeconds(60));
        //
        channel = new Channel(Publisher.METABROADCAST, "channel1", "k1", MediaType.AUDIO, "channel1");
        channel.setId(1l);
        //
        store.write(channel);
    }
    
    @Test
    public void testChannelById() {
        assertEquals(channel, store.fromId(1l).requireValue());
    }
    
    @Test
    public void testChannelByUri() {
        assertEquals(channel, store.fromUri("channel1").requireValue());
    }
    
    @Test
    public void testChannelByKey() {
        assertEquals(channel, store.fromKey("k1").requireValue());
    }
}
