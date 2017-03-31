package org.atlasapi.media.channel;

public interface ChannelGroupWriter {
    
    ChannelGroup createOrUpdate(ChannelGroup channelGroup);

    void deleteChannelGroupById(int channelGroupId);
}
