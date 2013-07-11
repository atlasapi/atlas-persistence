package org.atlasapi.media.channel;

import static org.atlasapi.media.channel.ChannelTranslator.IMAGES;
import static org.atlasapi.media.channel.ChannelTranslator.NEW_IMAGES;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.atlasapi.media.channel.Channel.Builder;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageTheme;
import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.LocalDate;
import org.junit.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;


public class DoubleImageWritingTest {
    
    private TemporalTitleTranslator titleTranslator = new TemporalTitleTranslator();
    private TemporalImageTranslator imageTranslator = new TemporalImageTranslator();
    
    private final ChannelTranslator translator = new ChannelTranslator();

    @Test
    public void testWritingOfPrimaryImages() {
        LocalDate now = new LocalDate();
        
        Image lightOpaque1 = createImage("lightOpaqueUri", ImageTheme.LIGHT_OPAQUE);
        Image lightOpaque2 = createImage("lightOpaqueUri2", ImageTheme.LIGHT_OPAQUE);
        Image other1 = createImage("darkOpaqueUri", ImageTheme.DARK_OPAQUE);
        Image other2 = createImage("darkTransparentUri", ImageTheme.DARK_TRANSPARENT);
        
        Builder channel = createChannel()
            .withImage(lightOpaque1, now.minusYears(1), now.plusYears(1))
            .withImage(lightOpaque2, now.minusYears(2), now.minusYears(2))
            .withImage(other1, now.minusYears(2), now.plusYears(1))
            .withImage(other2, now.minusYears(2), now.plusYears(2));
        
        DBObject translated = translator.toDBObject(null, channel.build());

        assertTrue(translated.containsField(IMAGES));
        assertTrue(translated.containsField(NEW_IMAGES));
        
        @SuppressWarnings("unchecked")
        Iterable<DBObject> images = (Iterable<DBObject>) TranslatorUtils.toDBObject(translated, IMAGES);
        assertThat(Iterables.size(images), is(2));
        
        Iterable<String> primaryImageUris = Iterables.transform(images, new Function<DBObject, String>() {
            @Override
            public String apply(DBObject input) {
                TemporalField<String> image = titleTranslator.fromDBObject(input);
                return image.getValue();
            }
        });
        
        assertEquals(
            ImmutableSet.of("lightOpaqueUri", "lightOpaqueUri2"), 
            ImmutableSet.copyOf(primaryImageUris)
        );
        
        @SuppressWarnings("unchecked")
        Iterable<DBObject> newImages = (Iterable<DBObject>) TranslatorUtils.toDBObject(translated, NEW_IMAGES);
        assertThat(Iterables.size(newImages), is(4));
        
        Iterable<String> imageUris = Iterables.transform(newImages, new Function<DBObject, String>() {
            @Override
            public String apply(DBObject input) {
                TemporalField<Image> image = imageTranslator.fromDBObject(input);
                return image.getValue().getCanonicalUri();
            }
        });
        
        assertEquals(
            ImmutableSet.of("lightOpaqueUri", "lightOpaqueUri2", "darkOpaqueUri", "darkTransparentUri"), 
            ImmutableSet.copyOf(imageUris)
        );
    }

    private Builder createChannel() {
        return Channel.builder()
            .withMediaType(MediaType.VIDEO)
            .withSource(Publisher.METABROADCAST);
    }

    private Image createImage(String uri, ImageTheme theme) {
        Image image = new Image(uri);
        image.setTheme(theme);
        return image;
    }

}
