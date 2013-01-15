package org.atlasapi.persistence.media.channel;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;

import com.google.common.base.Optional;

public interface ChannelGroupResolver {
    
     Optional<ChannelGroup> fromAlias(String alias);
     
     Optional<ChannelGroup> channelGroupFor(Long id);
     
     Iterable<ChannelGroup> channelGroupsFor(Iterable<Long> ids);
     
     Iterable<ChannelGroup> channelGroups();
     
     Iterable<ChannelGroup> channelGroupsFor(Channel channel);
}
