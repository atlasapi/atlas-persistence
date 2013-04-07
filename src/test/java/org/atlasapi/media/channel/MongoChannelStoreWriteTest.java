package org.atlasapi.media.channel;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

public class MongoChannelStoreWriteTest {
    
    private ChannelGroupStore channelGroupStore;
    private MongoChannelStore channelStore;
    
    @Before
    public void setUp() throws InterruptedException {
        DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
        channelGroupStore = new MongoChannelGroupStore(mongo);
        channelStore = new MongoChannelStore(mongo, channelGroupStore, channelGroupStore);
        channelStore.createOrUpdate(channel("uri1", "key1", MediaType.VIDEO, null, "test/1", "test/2"));
    }
    
    @Test
    public void testInsertNewChannel() {
        Maybe<Channel> maybeChannel = channelStore.fromUri("uri1");
        assertTrue(maybeChannel.hasValue());
        
        Channel channel = maybeChannel.requireValue();
        
        assertEquals("uri1", channel.uri());
        assertEquals("key1", channel.key());
        assertEquals(MediaType.VIDEO, channel.mediaType());
        assertEquals(ImmutableSet.of("test/1", "test/2"), channel.getAliasUrls());
    }
    
    @Test
    public void testUpdateExistingChannel() {
        Maybe<Channel> maybeChannel = channelStore.fromUri("uri1");
        Channel channel = maybeChannel.requireValue();
        
        // update and rewrite channel
        channel.setCanonicalUri("uri2");
        channel.setKey("key2");
        channel.setMediaType(MediaType.AUDIO);
        channel.setAliasUrls(ImmutableList.of("newAlias"));
        channel = channelStore.createOrUpdate(channel);
        
        assertEquals("uri2", channel.uri());
        assertEquals("key2", channel.key());
        assertEquals(MediaType.AUDIO, channel.mediaType());
        assertEquals(ImmutableSet.of("newAlias"), channel.getAliasUrls());
    }
    
    @Test
    public void testChangeOfParent() {
        Channel oldParent = channelStore.createOrUpdate(channel("parent1", "key1", MediaType.VIDEO, null, "test/1"));
        Id oldParentId = oldParent.getId();
        
        Channel newParent = channelStore.createOrUpdate(channel("parent2", "key2", MediaType.VIDEO, null, "test/2"));
        Id newParentId = newParent.getId();
        
        Channel child = channelStore.createOrUpdate(channel("child", "key3", MediaType.VIDEO, oldParentId, ""));
        oldParent = channelStore.fromId(oldParentId).requireValue();
        newParent = channelStore.fromId(newParentId).requireValue();
        
        assertEquals(oldParentId, child.parent());
        assertEquals(ImmutableSet.of(child.getId()), oldParent.variations());
        assertEquals(ImmutableSet.of(), newParent.variations());
        
        child.setParent(newParentId);
        child = channelStore.createOrUpdate(child);
        oldParent = channelStore.fromId(oldParentId).requireValue();
        newParent = channelStore.fromId(newParentId).requireValue();
        
        assertEquals(newParentId, child.parent());
        assertEquals(ImmutableSet.of(), oldParent.variations());
        assertEquals(ImmutableSet.of(child.getId()), newParent.variations());
    }
    
    @Test
    public void testChannelNumberings() {
        ChannelGroup oldGroup = new Platform();
        ChannelGroup oldAndNewGroup = new Platform();
        ChannelGroup newGroup = new Platform();
        
        Id oldGroupId = channelGroupStore.createOrUpdate(oldGroup).getId();
        Id oldAndNewGroupId = channelGroupStore.createOrUpdate(oldAndNewGroup).getId();
        Id newGroupId = channelGroupStore.createOrUpdate(newGroup).getId();
        
        Channel channel = channelStore.createOrUpdate(channel("channel", "key1", MediaType.VIDEO, null, "test/1"));
        
        ChannelNumbering oldNumbering = ChannelNumbering.builder()
            .withChannel(channel)
            .withChannelGroup(oldGroup)
            .withChannelNumber("1")
            .build();
        
        ChannelNumbering oldAndNewNumbering = ChannelNumbering.builder()
                .withChannel(channel)
                .withChannelGroup(oldAndNewGroup)
                .withChannelNumber("2")
                .build();
        
        ChannelNumbering newNumbering = ChannelNumbering.builder()
                .withChannel(channel)
                .withChannelGroup(newGroup)
                .withChannelNumber("3")
                .build();
        
        channel.addChannelNumber(oldNumbering);
        channel.addChannelNumber(oldAndNewNumbering);
        
        channel = channelStore.createOrUpdate(channel);
        oldGroup = channelGroupStore.channelGroupFor(oldGroupId).get();
        oldAndNewGroup = channelGroupStore.channelGroupFor(oldAndNewGroupId).get();
        newGroup = channelGroupStore.channelGroupFor(newGroupId).get();
        
        assertThat(channel.channelNumbers().size(), is(2));
        assertThat(oldGroup.getChannelNumberings().size(), is(1));
        assertThat(oldAndNewGroup.getChannelNumberings().size(), is(1));
        assertTrue(newGroup.getChannelNumberings().isEmpty());
        
        channel.setChannelNumbers(ImmutableList.<ChannelNumbering>of());
        channel.addChannelNumber(oldAndNewNumbering);
        channel.addChannelNumber(newNumbering);

        channel = channelStore.createOrUpdate(channel);
        oldGroup = channelGroupStore.channelGroupFor(oldGroupId).get();
        oldAndNewGroup = channelGroupStore.channelGroupFor(oldAndNewGroupId).get();
        newGroup = channelGroupStore.channelGroupFor(newGroupId).get();

        assertThat(channel.channelNumbers().size(), is(2));
        assertTrue(oldGroup.getChannelNumberings().isEmpty());
        assertThat(oldAndNewGroup.getChannelNumberings().size(), is(1));
        assertThat(newGroup.getChannelNumberings().size(), is(1));
    }

    private Channel channel(String uri, String key, MediaType mediaType, Id parent, String... alias) {
        Channel channel = Channel.builder()
            .withUri(uri)
            .withKey(key)
            .withSource(Publisher.BBC)
            .withMediaType(mediaType)
            .withParent(parent)
            .build();
        channel.setAliasUrls(ImmutableSet.copyOf(alias));
        return channel;
    }
}
