package org.atlasapi.media.channel;

import java.util.Map;

import com.metabroadcast.common.base.Maybe;

public interface ChannelResolver {

    @Deprecated
    Maybe<Channel> fromKey(String key);

    Maybe<Channel> fromId(long id);

    Maybe<Channel> fromUri(String uri);

    Iterable<Channel> forIds(Iterable<Long> ids);

    Iterable<Channel> all();

    Iterable<Channel> allChannels(ChannelQuery query);

    Maybe<Channel> forAlias(String alias);

    /**
     * Return a map whose keys are aliases prefixed with aliasPrefix
     *
     * @param aliasPrefix
     * @return
     */
    Map<String, Channel> forAliases(String aliasPrefix);

}
