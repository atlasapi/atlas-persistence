package org.atlasapi.persistence.media.channel;

import com.google.common.base.Optional;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;

public interface ChannelGroupStore extends ChannelGroupLister {

    Optional<ChannelGroup> channelGroupFor(Long id);
    
    Iterable<ChannelGroup> channelGroupsFor(Iterable<Long> ids);
        
    Iterable<ChannelGroup> channelGroupsFor(Channel channel);
    
    ChannelGroup store(ChannelGroup group);
    
}