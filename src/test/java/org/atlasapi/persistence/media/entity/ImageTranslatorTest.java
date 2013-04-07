package org.atlasapi.persistence.media.entity;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.ImageBackground;
import org.atlasapi.media.entity.ImageColor;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import com.metabroadcast.common.media.MimeType;

public class ImageTranslatorTest {

    private final ImageTranslator imageTranslator = new ImageTranslator();

    @Test
    public void testEncodeAndDecode() {
        Image image = createImage();
        Image decodedImage = imageTranslator.fromDBObject(imageTranslator.toDBObject(null, image),
                null);

        assertThat(decodedImage.getCanonicalUri(), is(equalTo(image.getCanonicalUri())));
        assertThat(decodedImage.getAvailabilityStart(), is(equalTo(image.getAvailabilityStart())));
        assertThat(decodedImage.getAvailabilityEnd(), is(equalTo(image.getAvailabilityEnd())));
        assertThat(decodedImage.getBackground(), is(equalTo(image.getBackground())));
        assertThat(decodedImage.getColor(), is(equalTo(image.getColor())));
        assertThat(decodedImage.getHeight(), is(equalTo(image.getHeight())));
        assertThat(decodedImage.getWidth(), is(equalTo(image.getWidth())));
        assertThat(decodedImage.getMimeType(), is(equalTo(image.getMimeType())));

    }

    private Image createImage() {

        Image image = new Image("http://example.com");
        image.setId(1);
        image.setAvailabilityStart(new DateTime(2013, DateTimeConstants.FEBRUARY, 1, 0, 0, 0, 0).withZone(DateTimeZone.UTC));
        image.setAvailabilityEnd(new DateTime(2013, DateTimeConstants.FEBRUARY, 2, 0, 0, 0, 0).withZone(DateTimeZone.UTC));
        image.setBackground(ImageBackground.BLACK);
        image.setColor(ImageColor.BLACK_AND_WHITE);
        image.setHeight(300);
        image.setWidth(200);
        image.setMimeType(MimeType.IMAGE_JPG);

        return image;
    }
}
