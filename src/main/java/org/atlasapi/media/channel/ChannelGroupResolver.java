package org.atlasapi.media.channel;

import com.google.common.base.Optional;

public interface ChannelGroupResolver {
    
     Optional<ChannelGroup> fromAlias(String alias);
     
     Optional<ChannelGroup> channelGroupFor(String canonicalUri);
     
     Optional<ChannelGroup> channelGroupFor(Long id);
     
     Iterable<ChannelGroup> channelGroupsFor(Iterable<? extends Long> ids);
     
     Iterable<ChannelGroup> channelGroups();
     
     Iterable<ChannelGroup> channelGroupsFor(Channel channel);

}
