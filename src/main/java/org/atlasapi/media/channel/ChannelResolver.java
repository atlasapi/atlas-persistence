package org.atlasapi.media.channel;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.metabroadcast.common.base.Maybe;

import java.util.Map;

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

    /**
     * A version of {@link #forAliases} that copes with multiple channels with the same alias.
     * <p>The default implementation just wraps {@code forAliases} in a multimap view:
     * <pre>
     *     return {@link Multimaps#forMap}(forAliases(aliasPrefix));
     * </pre>
     * </p>
     * @param aliasPrefix
     * @return
     */
    default Multimap<String, Channel> allForAliases(String aliasPrefix) {
        return Multimaps.forMap(forAliases(aliasPrefix));
    }

    Iterable<Channel> forKeyPairAlias(ChannelQuery channelQuery);

    default void refreshCache() {
        /* no-op */
    }
}
