package org.atlasapi.media.channel;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Pattern;

public abstract class BaseChannelStore implements ChannelStore {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    public Map<String, Channel> forAliases(String aliasPrefix) {
        final Pattern prefixPattern = Pattern.compile(String.format(
                "^%s",
                Pattern.quote(aliasPrefix)
        ));

        Iterable<Channel> channels = all();

        Map<String, Channel> channelMap = Maps.newHashMap();
        for (Channel channel : channels) {
            for (String alias : Iterables.filter(
                    channel.getAliasUrls(),
                    Predicates.contains(prefixPattern)
            )) {
                if (channelMap.get(alias) == null) {
                    channelMap.put(alias, channel);
                } else {
                    log.error("duplicate alias {} on channels {} & {}",
                            new Object[] {alias, channelMap.get(alias).getId(), channel.getId()});
                }
            }
        }
        return ImmutableMap.copyOf(channelMap);
    }

    public Multimap<String, Channel> allForAliases(String aliasPrefix) {
        final Pattern prefixPattern = Pattern.compile(String.format(
                "^%s",
                Pattern.quote(aliasPrefix)
        ));

        ImmutableMultimap.Builder<String, Channel> channelMap = ImmutableMultimap.builder();
        for (Channel channel : all()) {
            for (String alias : Iterables.filter(
                    channel.getAliasUrls(),
                    Predicates.contains(prefixPattern)
            )) {
                channelMap.put(alias, channel);
            }
        }
        return channelMap.build();
    }
}
