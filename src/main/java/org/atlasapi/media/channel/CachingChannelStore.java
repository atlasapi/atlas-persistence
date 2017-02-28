package org.atlasapi.media.channel;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.caching.BackgroundComputingValue;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.stream.MoreCollectors;

import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.IDS_NAMESPACE;
import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.IDS_VALUE;

public class CachingChannelStore implements ChannelStore, ServiceChannelStore {

    private final ChannelStore delegate;
    private final BackgroundComputingValue<List<Channel>> channels;
    private final Logger log = LoggerFactory.getLogger(CachingChannelStore.class);
    
    public CachingChannelStore(ChannelStore delegate) {
        this.delegate = delegate;
        channels = new BackgroundComputingValue<List<Channel>>(Duration.standardMinutes(5), new ChannelsUpdater(delegate));
    }
    @Override
    public void start() {
        channels.start();
    }

    @Override
    public void shutdown() {
        channels.shutdown();
    }

    @Override
    public Channel createOrUpdate(Channel channel) {
        return delegate.createOrUpdate(channel);
    }

    @Override
    @Deprecated
    public Maybe<Channel> fromKey(String key) {
        for (Channel channel : channels.get()) {
            if (channel.getKey().equals(key)) { 
                return Maybe.just(channel);
            }
        }
        return Maybe.nothing();
    }

    @Override
    public Maybe<Channel> fromId(long id) {
        for (Channel channel : channels.get()) {
            if (channel.getId().equals(id)) { 
                return Maybe.just(channel);
            }
        }
        return Maybe.nothing();
    }

    @Override
    public Maybe<Channel> fromUri(String uri) {
        for (Channel channel : channels.get()) {
            if (channel.getUri().equals(uri)) { 
                return Maybe.just(channel);
            }
        }
        return Maybe.nothing();
    }

    @Override
    public Iterable<Channel> forIds(final Iterable<Long> ids) {
        return Iterables.filter(channels.get(), new Predicate<Channel>() {
                @Override
                public boolean apply(Channel input) {
                    return Iterables.contains(ids, input.getId());
                }
            });
    }

    @Override
    public Iterable<Channel> all() {
        return channels.get();
    }

    @Override
    public Iterable<Channel> allChannels(ChannelQuery query) {
        return delegate.allChannels(query);
    }

    @Override
    public Maybe<Channel> forAlias(String alias) {
        for (Channel channel : channels.get()) {
            if (channel.getAliasUrls().contains(alias)) { 
                return Maybe.just(channel);
            }
        }
        return Maybe.nothing();
    }

    @Override
    public Map<String, Channel> forAliases(String aliasPrefix) {
        final Pattern prefixPattern = Pattern.compile(String.format("^%s", Pattern.quote(aliasPrefix)));

        Map<String, Channel> channelMap = Maps.newHashMap();
        for (Channel channel : channels.get()) {
            for (String alias : Iterables.filter(channel.getAliasUrls(), Predicates.contains(prefixPattern))) {
                if (channelMap.get(alias) == null) {
                    channelMap.put(alias, channel);    
                } else {
                    log.error("Duplicate alias " + alias + " on channels " + channel.getId() + " & " + channelMap.get(alias).getId());
                }
            }
        }
        return ImmutableMap.copyOf(channelMap);
    }

    // this method fetches channels by its aliases that are stored as ids in Mongo
    @Override
    public Iterable<Channel> forKeyPairAlias(ChannelQuery channelQuery) {
        return delegate.forKeyPairAlias(channelQuery);
    }

    private static class ChannelsUpdater implements Callable<List<Channel>> {
        private final ChannelResolver delegate;
        
        public ChannelsUpdater(ChannelResolver delegate) {
            this.delegate = delegate;
        }

        @Override
        public List<Channel> call() throws Exception {
            return ImmutableList.copyOf(delegate.all());
        }
        
    }
}
