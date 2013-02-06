package org.atlasapi.persistence.media.channel;

import org.atlasapi.media.channel.ChannelGroup;

public interface ChannelGroupWriter {
    
    ChannelGroup createOrUpdate(ChannelGroup channelGroup);
}
