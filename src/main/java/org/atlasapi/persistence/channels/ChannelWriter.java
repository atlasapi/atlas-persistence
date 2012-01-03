package org.atlasapi.persistence.channels;

import org.atlasapi.media.entity.Channel;

public interface ChannelWriter {

	void write(Channel channel);
	
}
