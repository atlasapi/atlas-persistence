package org.atlasapi.persistence.media.channel;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import org.atlasapi.media.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    
    private final LoadingCache<Long, Maybe<Channel>> channelIdCache = CacheBuilder.newBuilder()
        .build(new CacheLoader<Long, Maybe<Channel>>() {
            @Override
            public Maybe<Channel> load(Long id) throws Exception {
                Maybe<Channel> result = delegate.fromId(id);
                if (result.hasValue()) {
                    channelKeyCache.put(result.requireValue().key(), result);
                    channelUriCache.put(result.requireValue().uri(), result);
                }
                return result;
            }
        });
    
    private final LoadingCache<String, Maybe<Channel>> channelKeyCache = CacheBuilder.newBuilder()
        .build(new CacheLoader<String, Maybe<Channel>>() {
            @Override
            public Maybe<Channel> load(String key) throws Exception {
                Maybe<Channel> result = delegate.fromKey(key);
                if (result.hasValue()) {
                    channelIdCache.put(result.requireValue().getId(), result);
                    channelUriCache.put(result.requireValue().uri(), result);
                }
                return result;
            }
        });
    
    private final LoadingCache<String, Maybe<Channel>> channelUriCache = CacheBuilder.newBuilder()
        .build(new CacheLoader<String, Maybe<Channel>>() {
            @Override
            public Maybe<Channel> load(String uri) throws Exception {
                Maybe<Channel> result = delegate.fromUri(uri);
                if (result.hasValue()) {
                    channelIdCache.put(result.requireValue().getId(), result);
                    channelKeyCache.put(result.requireValue().key(), result);
                }
                return result;
            }
        });
    
    private final ChannelStore delegate;
    private final Logger log = LoggerFactory.getLogger(CachingChannelStore.class);
    
    public CachingChannelStore(ChannelStore delegate) {
        this.delegate = delegate;
        for (Channel channel : delegate.all()) {
            channelIdCache.put(channel.getId(), Maybe.just(channel));
            channelKeyCache.put(channel.key(), Maybe.just(channel));
            channelUriCache.put(channel.uri(), Maybe.just(channel));
        }
    }

    @Override
    public Channel createOrUpdate(Channel channel) {
        Channel written = delegate.createOrUpdate(channel);
        checkNotNull(written.getId(), "id null for channel " + channel.title());
        checkNotNull(written.key(), "key null for channel " + channel.title());
        checkNotNull(written.uri(), "uri null for channel " + channel.title());
        channelIdCache.put(written.getId(), Maybe.just(written));
        channelKeyCache.put(written.key(), Maybe.just(written));
        channelUriCache.put(written.uri(), Maybe.just(written));
        return written;
    }

    @Override
    @Deprecated
    public Maybe<Channel> fromKey(String key) {
        try {
            return channelKeyCache.get(key);
        } catch (ExecutionException e) {
            Throwables.propagate(e);
            // will never reach here
            return null;
        }
    }

    @Override
    public Maybe<Channel> fromId(long id) {
        try {
            return channelIdCache.get(id);
        } catch (ExecutionException e) {
            Throwables.propagate(e);
            // will never reach here
            return null;
        }
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
    public Iterable<Channel> forIds(Iterable<Long> ids) {
        try {
            return Iterables.transform(channelIdCache.getAll(ids).values(),
                new Function<Maybe<Channel>, Channel>() {
                    @Override
                    public Channel apply(Maybe<Channel> input) {
                        if (input.hasValue()) {
                            return input.requireValue();
                        }
                        throw new NullPointerException("Channel not found");
                    }
                });
        } catch (ExecutionException e) {
            Throwables.propagate(e);
            // will never reach here
            return null;
        }
    }

    @Override
    public Iterable<Channel> all() {
        return Iterables.filter(Iterables.transform(channelIdCache.asMap().values(), 
            new Function<Maybe<Channel>, Channel>() {
                @Override
                public Channel apply(Maybe<Channel> input) {
                    if (input.hasValue()) {
                        return input.requireValue();
                    }
                    return null;
                }
            }), Predicates.notNull());
    }

    @Override
    public Maybe<Channel> forAlias(String alias) {
        for (Maybe<Channel> channel : channelIdCache.asMap().values()) {
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
        for (Maybe<Channel> channel : channelIdCache.asMap().values()) {
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
