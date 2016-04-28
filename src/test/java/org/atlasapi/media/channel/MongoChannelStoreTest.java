package org.atlasapi.media.channel;

import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MongoChannelStoreTest {

    private ChannelGroupStore channelGroupStore;
    private MongoChannelStore channelStore;

    @Before
    public void setUp() throws Exception {
        DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
        channelGroupStore = new MongoChannelGroupStore(mongo);
        channelStore = new MongoChannelStore(mongo, channelGroupStore, channelGroupStore);
    }

    @Test
    public void testIfCheckChannelGroupReferencesChannelWhenReferencingChannelGroupFromChannel() {
        ChannelGroup group = new Platform();
        Channel channel = createChannel("bbc", "key", MediaType.VIDEO, null, "alias");

        Long groupId = channelGroupStore.createOrUpdate(group).getId();
        Long channelId = channelStore.createOrUpdate(channel).getId();

        channel.setChannelNumbers(ImmutableList.of(
                ChannelNumbering.builder()
                        .withChannelGroup(channelId)
                        .build()
        ));

        Channel updatedChannel = channelStore.createOrUpdate(channel);
        assertThat(updatedChannel.getChannelNumbers().size(), is(1));

        ChannelGroup channelGroup = channelGroupStore.channelGroupFor(groupId).get();
        assertThat(channelGroup.getChannelNumberings().size(), is(1));
    }

    @Test
    public void testIfChannelIsRemovedFromChannelGroupWhenReferenceToChannelGroupHasBeenRemovedFromChannel() {
        ChannelGroup group = new Platform();
        Channel channel = createChannel("bbc", "key", MediaType.VIDEO, null, "alias");

        Long groupId = channelGroupStore.createOrUpdate(group).getId();
        Long channelId = channelStore.createOrUpdate(channel).getId();

        channel.setChannelNumbers(ImmutableList.of(
                ChannelNumbering.builder()
                        .withChannelGroup(channelId)
                        .build()
        ));

        Channel updatedChannel = channelStore.createOrUpdate(channel);
        updatedChannel.setChannelNumbers(ImmutableList.<ChannelNumbering>of());

        Channel channelWithoutGroup = channelStore.createOrUpdate(updatedChannel);
        assertThat(channelWithoutGroup.getChannelNumbers().size(), is(0));

        ChannelGroup channelGroupWithoutChannel = channelGroupStore.channelGroupFor(groupId).get();
        assertThat(channelGroupWithoutChannel.getChannelNumberings().size(), is(0));
    }

    private Channel createChannel(String uri, String key, MediaType mediaType, Long parent, String... alias) {
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