package org.atlasapi.media.channel;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import org.atlasapi.media.content.MediaType;
import org.atlasapi.media.content.Publisher;
import org.junit.Test;

import com.mongodb.DBObject;

public class ChannelTranslatorTest {

    private final ChannelTranslator channelTranslator = new ChannelTranslator();
    
    @Test
    public void testEncodesAndDecodesChannels() {
        
        Channel channel = new Channel();
        
        channel.setTitle("title");
        channel.setKey("key");
        channel.setMediaType(MediaType.AUDIO);
        channel.setPublisher(Publisher.BBC);
        
        DBObject encoded = channelTranslator.toDBObject(null, channel);
        
        Channel decoded = channelTranslator.fromDBObject(encoded, null);
        
        assertThat(decoded, is(equalTo(channel)));
        assertThat(decoded.title(), is(equalTo(channel.title())));
        assertThat(decoded.key(), is(equalTo(channel.key())));
        assertThat(decoded.mediaType(), is(equalTo(channel.mediaType())));
        assertThat(decoded.publisher(),is(equalTo(channel.publisher())));
        
    }

}
