package org.atlasapi.media.channel;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;

import com.metabroadcast.common.stream.MoreCollectors;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static com.google.common.base.Preconditions.checkNotNull;

public class CachingChannelGroupStore implements ChannelGroupStore {

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
        try {
            return Optional.presentInstances(groupsByIdCache.getAll(ids).values());
        } catch (ExecutionException e) {
            throw new IllegalStateException(e);
        }
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
