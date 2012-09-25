package org.atlasapi.persistence.media.channel;

import org.atlasapi.media.channel.Channel;

/**
 */
public interface ChannelLister {
    
    Iterable<Channel> all();
}
