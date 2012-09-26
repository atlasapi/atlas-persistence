package org.atlasapi.persistence.media.channel;

import org.atlasapi.media.channel.ChannelGroup;

/**
 */
public interface ChannelGroupLister {
    
    Iterable<ChannelGroup> channelGroups();
}
