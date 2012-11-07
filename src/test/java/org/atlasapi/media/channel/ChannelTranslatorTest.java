package org.atlasapi.media.channel;

import org.atlasapi.persistence.media.channel.ChannelTranslator;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.mongodb.DBObject;

public class ChannelTranslatorTest {

    private final ChannelTranslator channelTranslator = new ChannelTranslator();
    
    @Test
    public void testEncodesAndDecodesChannels() {
        
        Channel channel = Channel.builder()
            .withSource(Publisher.BBC)
            .withUri("uri")
            .withKey("key")
            .withBroadcaster(Publisher.BBC)
            .withTitle("title")
            .withImage("image")
            .withMediaType(MediaType.AUDIO)
            .withHighDefinition(false)
            .withAvailableFrom(ImmutableSet.of(Publisher.BBC))
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
        assertThat(decoded.image(), is(equalTo(channel.image())));
        
    }

}
