package org.atlasapi.media.channel;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class CachingChannelGroupStore implements ChannelGroupStore {

    private static final Logger log = LoggerFactory.getLogger(CachingChannelGroupStore.class);

    private final LoadingCache<Long, Optional<ChannelGroup>> groupsByIdCache =
            CacheBuilder.newBuilder()
                        .expireAfterWrite(5, TimeUnit.MINUTES)
                        .maximumSize(500)
                        .build(new CacheLoader<Long, Optional<ChannelGroup>>() {

                            @Override
                            public Optional<ChannelGroup> load(Long key) throws Exception {
                                return delegate.channelGroupFor(key);
                            }

                            @Override
                            public Map<Long, Optional<ChannelGroup>> loadAll(
                                    Iterable<? extends Long> keys
                            ) {
                                return StreamSupport.stream(
                                        delegate.channelGroupsFor(keys).spliterator(), false
                                ).collect(MoreCollectors.toImmutableMap(
                                        ChannelGroup::getId,
                                        Optional::of
                                ));
                            }

                        });

    private final ChannelGroupStore delegate;

    public CachingChannelGroupStore(ChannelGroupStore delegate) {
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public Optional<ChannelGroup> fromAlias(String alias) {
        return delegate.fromAlias(alias);
    }

    @Override
    public Optional<ChannelGroup> channelGroupFor(Long id) {
        try {
            return groupsByIdCache.get(id);
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void invalidateCache(Long id) {
        groupsByIdCache.invalidate(id);
    }

    @Override
    public Iterable<ChannelGroup> channelGroupsFor(Iterable<? extends Long> ids) {
        return StreamSupport.stream(ids.spliterator(), false)
                .map(id -> {
                    try {
                        Optional<ChannelGroup> cacheChannelGroup = groupsByIdCache.get(id);
                        if (cacheChannelGroup.isPresent()) {
                            return cacheChannelGroup.get();
                        }
                    } catch (ExecutionException e) {
                        log.debug("Failed to get channel group from cache for ID {}", id);
                    }
                    Optional<ChannelGroup> dbChannelGroup = delegate.channelGroupFor(id);
                    if (dbChannelGroup.isPresent()) {
                        return dbChannelGroup.get();
                    }
                    throw new IllegalArgumentException(String.format("Channel Group not found for %s", id));
                })
                .collect(Collectors.toList());
        //            return Optional.presentInstances(groupsByIdCache.getAll(ids).values());
    }

    @Override
    public Iterable<ChannelGroup> channelGroups() {
        return delegate.channelGroups();
    }

    @Override
    public Iterable<ChannelGroup> channelGroupsFor(Channel channel) {
        return delegate.channelGroupsFor(channel);
    }

    @Override
    public ChannelGroup createOrUpdate(ChannelGroup channelGroup) {
        return delegate.createOrUpdate(channelGroup);
    }

    @Override
    public Optional<ChannelGroup> channelGroupFor(String canonicalUri) {
        return delegate.channelGroupFor(canonicalUri);
    }

    @Override
    public void deleteChannelGroupById(long channelGroupId) {
        delegate.deleteChannelGroupById(channelGroupId);
    }

}
