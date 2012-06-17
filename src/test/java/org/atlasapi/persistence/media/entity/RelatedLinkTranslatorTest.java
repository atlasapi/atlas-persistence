package org.atlasapi.persistence.media.entity;

import static org.atlasapi.media.content.RelatedLink.unknownTypeLink;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import org.atlasapi.media.content.RelatedLink;
import org.junit.Test;

import com.mongodb.DBObject;

public class RelatedLinkTranslatorTest {

    private final RelatedLinkTranslator translator = new RelatedLinkTranslator();

    @Test
    public void encodesAndDecodesRelatedLink() {

        RelatedLink link = unknownTypeLink("a related link")
                .withSourceId("source id")
                .withShortName("short name")
                .withTitle("title")
                .withDescription("desc")
                .withImage("image")
                .withThumbnail("thumb").build();

        DBObject dbo = translator.toDBObject(link);
        
        RelatedLink decoded = translator.fromDBObject(dbo);
        
        assertThat(decoded.getUrl(), is(equalTo(link.getUrl())));
        assertThat(decoded.getType(), is(equalTo(link.getType())));
        assertThat(decoded.getSourceId(), is(equalTo(link.getSourceId())));
        assertThat(decoded.getShortName(), is(equalTo(link.getShortName())));
        assertThat(decoded.getTitle(), is(equalTo(link.getTitle())));
        assertThat(decoded.getDescription(), is(equalTo(link.getDescription())));
        assertThat(decoded.getImage(), is(equalTo(link.getImage())));
        assertThat(decoded.getThumbnail(), is(equalTo(link.getThumbnail())));
        
    }

}
