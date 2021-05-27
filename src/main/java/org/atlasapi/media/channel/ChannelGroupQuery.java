package org.atlasapi.media.channel;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.media.entity.Publisher;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Set;

public class ChannelGroupQuery {

    @Nullable
    private final Set<Publisher> publishers;
    @Nullable
    private final Set<Long> channelGroupIds;
    @Nullable
    private final Set<Long> channelNumbersFromIds;

    private ChannelGroupQuery(Builder builder) {
        publishers = builder.publishers == null
                ? null
                : ImmutableSet.copyOf(builder.publishers);
        channelGroupIds = builder.channelGroupIds == null
                ? null
                : ImmutableSet.copyOf(builder.channelGroupIds);
        channelNumbersFromIds = builder.channelNumbersFromIds == null
                ? null
                : ImmutableSet.copyOf(builder.channelNumbersFromIds);
    }

    @Nullable
    public Set<Publisher> getPublishers() {
        return publishers;
    }

    @Nullable
    public Set<Long> getChannelGroupIds() {
        return channelGroupIds;
    }

    @Nullable
    public Set<Long> getChannelNumbersFromIds() {
        return channelNumbersFromIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ChannelGroupQuery that = (ChannelGroupQuery) o;
        return Objects.equals(publishers, that.publishers) && Objects.equals(channelGroupIds, that.channelGroupIds) && Objects.equals(channelNumbersFromIds, that.channelNumbersFromIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(publishers, channelGroupIds, channelNumbersFromIds);
    }

    @Override
    public String toString() {
        return "ChannelGroupQuery{" +
                "publishers=" + publishers +
                ", ids=" + channelGroupIds +
                ", channelNumbersFromIds=" + channelNumbersFromIds +
                '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Set<Publisher> publishers;
        private Set<Long> channelGroupIds;
        private Set<Long> channelNumbersFromIds;

        private Builder() {
        }

        public Builder withPublisher(Publisher publisher) {
            this.publishers = ImmutableSet.of(publisher);
            return this;
        }

        public Builder withPublishers(Set<Publisher> publishers) {
            this.publishers = publishers;
            return this;
        }

        public Builder withChannelGroupId(Long channelGroupId) {
            this.channelGroupIds = ImmutableSet.of(channelGroupId);
            return this;
        }

        public Builder withChannelGroupIds(Set<Long> channelGroupIds) {
            this.channelGroupIds = channelGroupIds;
            return this;
        }

        public Builder withChannelNumbersFromId(Long channelNumbersFromId) {
            this.channelNumbersFromIds = ImmutableSet.of(channelNumbersFromId);
            return this;
        }

        public Builder withChannelNumbersFromIds(Set<Long> channelNumbersFromIds) {
            this.channelNumbersFromIds = channelNumbersFromIds;
            return this;
        }

        public ChannelGroupQuery build() {
            return new ChannelGroupQuery(this);
        }
    }
}
