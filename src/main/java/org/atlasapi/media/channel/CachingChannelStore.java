package org.atlasapi.media.channel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.caching.BackgroundComputingValue;
import com.metabroadcast.common.stream.MoreCollectors;
import org.joda.time.Duration;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;

public class CachingChannelStore extends BaseChannelStore implements ServiceChannelStore {

    private final ChannelStore delegate;
    private final BackgroundComputingValue<List<Channel>> channels;

    public CachingChannelStore(ChannelStore delegate) {
        this.delegate = delegate;
        channels = new BackgroundComputingValue<>(
                Duration.standardMinutes(5),
                new ChannelsUpdater(delegate)
        );
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

    @Deprecated
    @Override
    public Maybe<Channel> fromKey(String key) {
        for (Channel channel : channels.get()) {
            if (Objects.equals(channel.getKey(), key)) {
                return Maybe.just(channel);
            }
        }
        return Maybe.nothing();
    }

    @SuppressWarnings("deprecation")    // specified by interface
    @Override
    public Maybe<Channel> fromId(long id) {
        for (Channel channel : channels.get()) {
            if (channel.getId().equals(id)) { 
                return Maybe.just(channel);
            }
        }
        return Maybe.nothing();
    }

    @SuppressWarnings("deprecation")    // specified by interface
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
        return channels.get()
                .stream()
                .filter(input -> Iterables.contains(ids, input.getId()))
                .collect(MoreCollectors.toImmutableList());
    }

    @Override
    public Iterable<Channel> all() {
        return channels.get();
    }

    @Override
    public Iterable<Channel> allChannels(ChannelQuery query) {
        return delegate.allChannels(query);
    }

    @SuppressWarnings("deprecation")    // specified by interface
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
    public void refreshCache() {
        channels.refreshCache();
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
