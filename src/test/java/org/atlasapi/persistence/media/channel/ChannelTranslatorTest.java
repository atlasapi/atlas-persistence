package org.atlasapi.persistence.media.channel;

import org.atlasapi.persistence.media.channel.ChannelTranslator;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Set;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelNumbering;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Duration;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.mongodb.DBObject;

public class ChannelTranslatorTest {

    private final ChannelTranslator channelTranslator = new ChannelTranslator();
    
    @Test
    public void testEncodesAndDecodesChannels() {
        
        ChannelNumbering channelNumber = ChannelNumbering.builder()
            .withChannelNumber("5")
            .withChannel(1234L)
            .withChannelGroup(5678L)
            .build();
        
        Set<Long> variations = ImmutableSet.of(2345L, 2346L, 2347L);
        
        Channel channel = Channel.builder()
            .withSource(Publisher.BBC)
            .withUri("uri")
            .withKey("key")
            .withBroadcaster(Publisher.BBC)
            .withTitle("title")
            .withImage("image")
            .withMediaType(MediaType.AUDIO)
            .withHighDefinition(false)
            .withHighDefinition(true)
            .withTimeshift(Duration.standardSeconds(3600))
            .withAvailableFrom(ImmutableSet.of(Publisher.BBC))
            .withChannelNumber(channelNumber)
            .withParent(2345L)
            .withVariationIds(variations)
            .build();
        
        DBObject encoded = channelTranslator.toDBObject(null, channel);
        
        Channel decoded = channelTranslator.fromDBObject(encoded, null);
        
        assertThat(decoded, is(equalTo(channel)));
        assertThat(decoded.title(), is(equalTo(channel.title())));
        assertThat(decoded.uri(), is(equalTo(channel.uri())));
        assertThat(decoded.key(), is(equalTo(channel.key())));
        assertThat(decoded.mediaType(), is(equalTo(channel.mediaType())));
        assertThat(decoded.source(),is(equalTo(channel.source())));
        assertThat(decoded.availableFrom(), is(equalTo(channel.availableFrom())));
        assertThat(decoded.highDefinition(), is(equalTo(channel.highDefinition())));
        assertThat(decoded.regional(), is(equalTo(channel.regional())));
        assertThat(decoded.timeshift(), is(equalTo(channel.timeshift())));
        assertThat(decoded.image(), is(equalTo(channel.image())));
        assertThat(decoded.parent(), is(equalTo(channel.parent())));
        assertThat(decoded.variations(), is(equalTo(channel.variations())));
        assertThat(decoded.channelNumbers(), is(equalTo(channel.channelNumbers())));
        
    }

}
