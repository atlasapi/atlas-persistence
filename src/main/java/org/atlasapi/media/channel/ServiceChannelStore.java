package org.atlasapi.media.channel;

public interface ServiceChannelStore extends ChannelStore {

    void start();

    void shutdown();

}