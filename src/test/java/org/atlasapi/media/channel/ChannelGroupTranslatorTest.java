package org.atlasapi.media.channel;

import org.atlasapi.persistence.media.channel.ChannelGroupTranslator;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

import org.atlasapi.media.channel.ChannelGroup.ChannelGroupType;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.metabroadcast.common.intl.Countries;
import com.mongodb.DBObject;

public class ChannelGroupTranslatorTest {

    private final ChannelGroupTranslator channelGroupTranslator = new ChannelGroupTranslator();
    
    @Test
    public void testEncodesAndDecodedChannelGroup() {
        
        ChannelGroup channelGroup = new ChannelGroup();
        channelGroup.setAvailableCountries(ImmutableSet.of(Countries.US,Countries.GB));
        channelGroup.setTitle("Title");
        channelGroup.setPublisher(Publisher.BBC);
        channelGroup.setChannels(Lists.transform(ImmutableList.of(1234L, 1235L, 1236L),Id.fromLongValue()));
        channelGroup.setType(ChannelGroupType.PLATFORM);
        
        DBObject encoded = channelGroupTranslator.toDBObject(null, channelGroup);
        
        ChannelGroup decoded = channelGroupTranslator.fromDBObject(encoded, null);
        
        assertThat(decoded.getAvailableCountries(), is(equalTo(channelGroup.getAvailableCountries())));
        assertThat(decoded.getPublisher(), is(equalTo(channelGroup.getPublisher())));
        assertThat(decoded.getTitle(), is(equalTo(channelGroup.getTitle())));
        assertThat(decoded.getChannels(), is(equalTo(channelGroup.getChannels())));
        
    }

}
