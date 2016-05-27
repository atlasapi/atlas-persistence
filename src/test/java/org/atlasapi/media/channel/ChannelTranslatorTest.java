package org.atlasapi.media.channel;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import java.util.Set;

import org.atlasapi.media.entity.ImageTheme;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.RelatedLink;
import org.joda.time.DateTime;
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
        
        RelatedLink relatedLink = RelatedLink.simulcastLink("simulcast_url").build();

        Image image = new Image("image");
        image.setTheme(ImageTheme.LIGHT_OPAQUE);
        
        Set<String> genres = ImmutableSet.of("Genre");
        DateTime dateTimeTest = DateTime.now();

        Channel channel = Channel.builder()
            .withSource(Publisher.BBC)
            .withUri("uri")
            .withKey("key")
            .withBroadcaster(Publisher.BBC)
            .withTitle("title")
            .withImage(image)
            .withMediaType(MediaType.AUDIO)
            .withHighDefinition(true)
            .withAdult(false)
            .withTimeshift(Duration.standardSeconds(3600))
            .withAvailableFrom(ImmutableSet.of(Publisher.BBC))
            .withChannelNumber(channelNumber)
            .withParent(2345L)
            .withVariationIds(variations)
            .withRelatedLink(relatedLink)
            .withGenres(genres)
            .withAdvertiseFrom(dateTimeTest)
            .withShortDescription("short")
            .withMediumDescription("medium")
            .withLongDescription("long")
            .withRegion("region")
            .withChannelType(ChannelType.CHANNEL)
            .withInteractive(true)
            .build();
        
        DBObject encoded = channelTranslator.toDBObject(null, channel);
        
        Channel decoded = channelTranslator.fromDBObject(encoded, null);
        
        assertThat(decoded, is(equalTo(channel)));
        assertThat(decoded.getTitle(), is(equalTo(channel.getTitle())));
        assertThat(decoded.getUri(), is(equalTo(channel.getUri())));
        assertThat(decoded.getKey(), is(equalTo(channel.getKey())));
        assertThat(decoded.getMediaType(), is(equalTo(channel.getMediaType())));
        assertThat(decoded.getSource(),is(equalTo(channel.getSource())));
        assertThat(decoded.getAvailableFrom(), is(equalTo(channel.getAvailableFrom())));
        assertThat(decoded.getHighDefinition(), is(equalTo(channel.getHighDefinition())));
        assertThat(decoded.getRegional(), is(equalTo(channel.getRegional())));
        assertThat(decoded.getAdult(), is(equalTo(channel.getAdult())));
        assertThat(decoded.getTimeshift(), is(equalTo(channel.getTimeshift())));
        assertThat(decoded.getImages(), is(equalTo(channel.getImages())));
        assertThat(decoded.getParent(), is(equalTo(channel.getParent())));
        assertThat(decoded.getVariations(), is(equalTo(channel.getVariations())));
        assertThat(decoded.getChannelNumbers(), is(equalTo(channel.getChannelNumbers())));
        assertThat(decoded.getRelatedLinks(), is(equalTo(channel.getRelatedLinks())));
        assertThat(decoded.getGenres(), is(equalTo(genres)));
        assertThat(decoded.getAdvertiseFrom().toDate(), is(equalTo(channel.getAdvertiseFrom().toDate())));
        assertThat(decoded.getShortDescription(), is(equalTo(channel.getShortDescription())));
        assertThat(decoded.getMediumDescription(), is(equalTo(channel.getMediumDescription())));
        assertThat(decoded.getLongDescription(), is(equalTo(channel.getLongDescription())));
        assertThat(decoded.getRegion(), is(equalTo(channel.getRegion())));
        assertThat(decoded.getChannelType(), is(equalTo(channel.getChannelType())));
        assertThat(decoded.getInteractive(), is(equalTo(channel.getInteractive())));
    }

    @Test
    public void interactiveSetToFalseByDefault() {
        Channel channel = Channel.builder()
                .withSource(Publisher.BBC)
                .withMediaType(MediaType.VIDEO)
                .build();

        DBObject encoded = channelTranslator.toDBObject(null, channel);

        Channel decoded = channelTranslator.fromDBObject(encoded, null);

        assertFalse(decoded.getInteractive());
    }
}
