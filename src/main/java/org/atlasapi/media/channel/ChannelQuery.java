package org.atlasapi.media.channel;

import java.util.Set;

import org.atlasapi.media.entity.MediaType;
import org.atlasapi.media.entity.Publisher;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkNotNull;


public class ChannelQuery {

    private final Optional<Publisher> broadcaster;
    private final Optional<MediaType> mediaType;
    private final Optional<Publisher> availableFrom;
    private final Optional<Set<Long>> channelGroups;
    private final Optional<Set<String>> genres;
    private final Optional<DateTime> advertisedOn;
    private final Optional<Publisher> publisher;
    private final Optional<String> uri;
    private final Optional<ChannelType> channelType;

    public static Builder builder() {
        return new Builder();
    }

    private ChannelQuery(
            Optional<Publisher> broadcaster,
            Optional<MediaType> mediaType,
            Optional<Publisher> availableFrom,
            Optional<Set<Long>> channelGroups,
            Optional<Set<String>> genres,
            Optional<DateTime> advertisedOn,
            Optional<Publisher> publisher,
            Optional<String> uri,
            Optional<ChannelType> channelType
    ) {
        this.broadcaster = checkNotNull(broadcaster);
        this.mediaType = checkNotNull(mediaType);
        this.availableFrom = checkNotNull(availableFrom);
        this.channelGroups = checkNotNull(channelGroups);
        this.genres = checkNotNull(genres);
        this.advertisedOn = checkNotNull(advertisedOn);
        this.publisher = checkNotNull(publisher);
        this.uri = checkNotNull(uri);
        this.channelType = checkNotNull(channelType);
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

    public Optional<Set<String>> getGenres() {
        return genres;
    }

    public Optional<DateTime> getAdvertisedOn() {
        return advertisedOn;
    }

    public Optional<Publisher> getPublisher() {
        return publisher;
    }

    public Optional<String> getUri() {
        return uri;
    }

    public Optional<ChannelType> getChannelType() {
        return channelType;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(ChannelQuery.class)
                .add("broadcaster", broadcaster)
                .add("mediaType", mediaType)
                .add("availableFrom", availableFrom)
                .add("channelGroups", channelGroups)
                .add("genres", genres)
                .add("advertiseOn", advertisedOn)
                .add("publisher", publisher)
                .add("uri", uri)
                .add("channelType", channelType)
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                broadcaster,
                mediaType,
                availableFrom,
                channelGroups,
                genres,
                publisher,
                uri,
                channelType
        );
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
                    && channelGroups.equals(other.channelGroups)
                    && genres.equals(other.genres)
                    && advertisedOn.equals(other.advertisedOn)
                    && publisher.equals(other.publisher)
                    && uri.equals(other.uri)
                    && channelType.equals(other.channelType);
        }
        return false;
    }

    public static class Builder {

        private Optional<Publisher> broadcaster = Optional.absent();
        private Optional<MediaType> mediaType = Optional.absent();
        private Optional<Publisher> availableFrom = Optional.absent();
        private Optional<Set<Long>> channelGroups = Optional.absent();
        private Optional<Set<String>> genres = Optional.absent();
        private Optional<DateTime> advertisedOn = Optional.absent();
        private Optional<Publisher> source = Optional.absent();
        private Optional<Publisher> publisher = Optional.absent();
        private Optional<String> uri = Optional.absent();
        private Optional<ChannelType> channelType = Optional.absent();

        private Builder() {}

        public ChannelQuery build() {
            return new ChannelQuery(
                    broadcaster,
                    mediaType,
                    availableFrom,
                    channelGroups,
                    genres,
                    advertisedOn,
                    publisher,
                    uri,
                    channelType
            );
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

        public Builder withGenres(Set<String> genres) {
            this.genres = Optional.fromNullable(genres);
            return this;
        }

        public Builder withAdvertisedOn(DateTime advertisedOn) {
            this.advertisedOn = Optional.fromNullable(advertisedOn);
            return this;
        }

        public Builder withPublisher(Publisher publisher) {
            this.publisher = Optional.fromNullable(publisher);
            return this;
        }

        public Builder withUri(String uri) {
            this.uri = Optional.fromNullable(uri);
            return this;
        }

        public Builder withChannelType(ChannelType channelType) {
            this.channelType = Optional.fromNullable(channelType);
            return this;
        }
    }
}
