package org.atlasapi.persistence.media.channel;

import java.util.Map;

import org.atlasapi.media.channel.Channel;

import com.metabroadcast.common.base.Maybe;
import org.atlasapi.media.channel.Channel;

public interface ChannelResolver extends ChannelLister {

	@Deprecated
	Maybe<Channel> fromKey(String key);

	Maybe<Channel> fromId(Long id);
	
	Maybe<Channel> fromUri(String uri);
	
	Iterable<Channel> forIds(Iterable<Long> ids);
		
	/**
	 * Return a map whose keys are aliases prefixed with aliasPrefix
	 * 
	 * @param aliasPrefix
	 * @return
	 */
	Map<String, Channel> forAliases(String aliasPrefix);
	
}
