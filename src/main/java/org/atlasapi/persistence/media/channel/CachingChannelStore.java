package org.atlasapi.persistence.media.channel;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.atlasapi.media.channel.Channel;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;

public class CachingChannelStore implements ChannelStore {

    private final ChannelStore delegate;
       
    private final LoadingCache<String, Maybe<Channel>> channelUriCache = CacheBuilder.newBuilder()
        .expireAfterWrite(5, TimeUnit.MINUTES)
        .build(new CacheLoader<String, Maybe<Channel>>() {
            @Override
            public Maybe<Channel> load(String uri) throws Exception {
                Maybe<Channel> result = delegate.fromUri(uri);
                return result;
            }
        });
    
    public CachingChannelStore(ChannelStore delegate) {
        this.delegate = delegate;
        for (Channel channel : delegate.all()) {
            channelUriCache.put(channel.uri(), Maybe.just(channel));
        }
    }

    @Override
    public Channel createOrUpdate(Channel channel) {
        Channel written = delegate.createOrUpdate(channel);
        checkNotNull(written.uri(), "uri null for channel " + channel.title());
        channelUriCache.put(written.uri(), Maybe.just(written));
        return written;
    }

    @Override
    @Deprecated
    public Maybe<Channel> fromKey(String key) {
        for (Maybe<Channel> channel : channelUriCache.asMap().values()) {
            if (channel.hasValue()) {
                if (channel.requireValue().key().equals(key)) { 
                    return channel;
                }
            }
        }
        return Maybe.nothing();
    }

    @Override
    public Maybe<Channel> fromId(long id) {
        for (Maybe<Channel> channel : channelUriCache.asMap().values()) {
            if (channel.hasValue()) {
                if (channel.requireValue().getId().equals(id)) { 
                    return channel;
                }
            }
        }
        return Maybe.nothing();
    }

    @Override
    public Maybe<Channel> fromUri(String uri) {
        try {
            return channelUriCache.get(uri);
        } catch (ExecutionException e) {
            Throwables.propagate(e);
            // will never reach here
            return null;
        }
    }

    @Override
    public Iterable<Channel> forIds(final Iterable<Long> ids) {
        return Iterables.filter(
            Iterables.transform(
            channelUriCache.asMap().values(), 
                new Function<Maybe<Channel>, Channel>() {
                    @Override
                    public Channel apply(Maybe<Channel> input) {
                        if (input.hasValue() && Iterables.contains(ids, input.requireValue().getId())) {
                            return input.requireValue();
                        }
                        return null;
                    }
                }
            ), Predicates.notNull());
    }

    @Override
    public Iterable<Channel> all() {
        return Iterables.filter(
            Iterables.transform(
            channelUriCache.asMap().values(), 
                new Function<Maybe<Channel>, Channel>() {
                    @Override
                    public Channel apply(Maybe<Channel> input) {
                        if (input.hasValue()) {
                            return input.requireValue();
                        }
                        return null;
                    }
                }
            ), Predicates.notNull());
    }

    @Override
    public Maybe<Channel> forAlias(String alias) {
        for (Maybe<Channel> channel : channelUriCache.asMap().values()) {
            if (channel.hasValue()) {
                // TODO new aliases
                for (String channelAlias : channel.requireValue().getAliasUrls()) {
                    if (alias.equals(channelAlias)) {
                        return channel;
                    }
                }
            }
        }
        return Maybe.nothing();
    }

    @Override
    public Map<String, Channel> forAliases(String aliasPrefix) {
        final Pattern prefixPattern = Pattern.compile(String.format("^%s", Pattern.quote(aliasPrefix)));

        Builder<String, Channel> channelMap = ImmutableMap.builder();
        for (Maybe<Channel> channel : channelUriCache.asMap().values()) {
            if (channel.hasValue()) {
                // TODO new aliases
                for (String alias : Iterables.filter(channel.requireValue().getAliasUrls(), Predicates.contains(prefixPattern))) {
                    channelMap.put(alias, channel.requireValue());
                }
            }
        }
        return channelMap.build();
    }

}
