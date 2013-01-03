package org.atlasapi.media.channel;

import com.google.common.base.Optional;

public interface ChannelGroupResolver {
     Optional<ChannelGroup> fromAlias(String alias);
     
     Iterable<ChannelGroup> forAliases(Iterable<String> aliases);
}
