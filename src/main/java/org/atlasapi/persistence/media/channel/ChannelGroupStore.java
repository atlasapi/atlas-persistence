package org.atlasapi.persistence.media.channel;

import com.google.common.base.Optional;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.media.common.Id;

public interface ChannelGroupStore extends ChannelGroupLister {

    Optional<ChannelGroup> channelGroupFor(Id id);
    
    Iterable<ChannelGroup> channelGroupsFor(Iterable<Id> ids);
        
    Iterable<ChannelGroup> channelGroupsFor(Channel channel);
    
    ChannelGroup store(ChannelGroup group);
    
}
