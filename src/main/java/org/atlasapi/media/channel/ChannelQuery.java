package org.atlasapi.media.channel;

import java.util.Set;

import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Objects;
import com.google.common.base.Optional;


public class ChannelQuery {

    private final Optional<Publisher> broadcaster;
    private final Optional<MediaType> mediaType;
    private final Optional<Publisher> availableFrom;
    private final Optional<Set<Long>> channelGroups;
    
    public static Builder builder() {
        return new Builder();
    }
    
    private ChannelQuery(Optional<Publisher> broadcaster, Optional<MediaType> mediaType, 
            Optional<Publisher> availableFrom, Optional<Set<Long>> channelGroups) {
                this.broadcaster = broadcaster;
                this.mediaType = mediaType;
                this.availableFrom = availableFrom;
                this.channelGroups = channelGroups;
    }
    
    public Optional<Publisher> getBroadcaster() {
        return broadcaster;
    }
    
    public Optional<MediaType> getMediaType() {
        return mediaType;
    }
    
    public Optional<Publisher> getAvailableFrom() {
        return availableFrom;
    }
    
    public Optional<Set<Long>> getChannelGroups() {
        return channelGroups;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(ChannelQuery.class)
                .add("broadcaster", broadcaster)
                .add("mediaType", mediaType)
                .add("availableFrom", availableFrom)
                .add("channelGroups", channelGroups)
                .toString();
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(broadcaster, mediaType, availableFrom, channelGroups);
    }
    
    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof ChannelQuery) {
            ChannelQuery other = (ChannelQuery) that;
            return broadcaster.equals(other.broadcaster)
                    && mediaType.equals(other.mediaType)
                    && availableFrom.equals(other.availableFrom)
                    && channelGroups.equals(other.channelGroups);
        }
        
        return false;
    }
    
    public static class Builder {
        
        private Optional<Publisher> broadcaster = Optional.absent();
        private Optional<MediaType> mediaType = Optional.absent();
        private Optional<Publisher> availableFrom = Optional.absent();
        private Optional<Set<Long>> channelGroups = Optional.absent();
        
        private Builder() {}
        
        public ChannelQuery build() {
            return new ChannelQuery(broadcaster, mediaType, availableFrom, channelGroups);
        }

        
        public Builder withBroadcaster(Publisher broadcaster) {
            this.broadcaster = Optional.fromNullable(broadcaster);
            return this;
        }
        
        public Builder withMediaType(MediaType mediaType) {
            this.mediaType = Optional.fromNullable(mediaType);
            return this;
        }
        
        public Builder withAvailableFrom(Publisher availableFrom) {
            this.availableFrom = Optional.fromNullable(availableFrom);
            return this;
        }
        
        public Builder withChannelGroups(Set<Long> channelGroups) {
            this.channelGroups = Optional.fromNullable(channelGroups);
            return this;
        }
    }
}
