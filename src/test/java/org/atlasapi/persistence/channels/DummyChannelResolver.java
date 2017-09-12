package org.atlasapi.persistence.channels;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelQuery;
import org.atlasapi.media.channel.ChannelResolver;

import java.util.Collection;
import java.util.Map;

public class DummyChannelResolver implements ChannelResolver {

	private Map<String, Channel> channels;
	
	public DummyChannelResolver(Iterable<Channel> channels) {
		 this.channels = Maps.uniqueIndex(channels, Channel::getUri);
	}
	
	public DummyChannelResolver(Map<String, Channel> channels) {
		this.channels = ImmutableMap.copyOf(channels); 
	}
	
	@Override
	public Maybe<Channel> fromKey(final String key) {
				
		Iterable<Channel> withKey = Iterables.filter(
				channels.values(),
				input -> key.equals(input.getKey())
		);
		
		if(Iterables.isEmpty(withKey)) {
			return Maybe.nothing();
		}
		else {
			return Maybe.just(withKey.iterator().next());
		}
	}

	@Override
	public Maybe<Channel> fromId(long id) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Maybe<Channel> fromUri(String uri) {
	    return Maybe.fromPossibleNullValue(channels.get(uri));
	}

	@Override
	public Collection<Channel> all() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<String, Channel> forAliases(String aliasPrefix) {
		throw new UnsupportedOperationException();
	}

    @Override
    public Iterable<Channel> forIds(Iterable<Long> ids) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Maybe<Channel> forAlias(String alias) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<Channel> allChannels(ChannelQuery query) {
        throw new UnsupportedOperationException();
    }

	@Override
	public Iterable<Channel> forKeyPairAlias(ChannelQuery channelQuery) {
		throw new UnsupportedOperationException();
	}
}
