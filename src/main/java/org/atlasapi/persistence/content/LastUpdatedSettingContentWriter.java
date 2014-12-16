package org.atlasapi.persistence.content;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Image;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Version;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;

public class LastUpdatedSettingContentWriter implements ContentWriter {

    private final Predicate<Identified> HAS_CANONICAL_URI = new Predicate<Identified>() {
        @Override public boolean apply(Identified input) {
            return !Strings.isNullOrEmpty(input.getCanonicalUri());
        }
    };

    private final Function<Identified, String> TO_CANONICAL_URI = new Function<Identified, String>() {
        @Override public String apply(Identified input) {
            return input.getCanonicalUri();
        }
    };

    private final ContentResolver resolver;
    private final ContentWriter writer;
    private final Clock clock;

    public LastUpdatedSettingContentWriter(ContentResolver resolver, ContentWriter writer, Clock clock) {
        this.resolver = resolver;
        this.writer = writer;
        this.clock = clock;
    }

    public LastUpdatedSettingContentWriter(ContentResolver resolver, ContentWriter writer) {
        this(resolver, writer, new SystemClock());
    }

    @Override
    public void createOrUpdate(Item item) {
        Maybe<Identified> previously = resolver.findByCanonicalUris(ImmutableList.of(item.getCanonicalUri())).get(item.getCanonicalUri());

        DateTime now = clock.now();
        if(previously.hasValue() && previously.requireValue() instanceof Item) {
            Item prevItem = (Item) previously.requireValue();
            if(!itemsEqual(prevItem, item)){
                item.setLastUpdated(now);
            }
            setUpdatedVersions(prevItem.getVersions(), item.getVersions(), now);
            setUpdatedClips(prevItem.getClips(), item.getClips(), now);
        }
        else {
            setUpdatedVersions(Sets.<Version>newHashSet(), item.getVersions(), now);
            setUpdatedClips(Lists.<Clip>newArrayList(), item.getClips(), now);
        }

        if(item.getLastUpdated() == null  || previously.isNothing()) {
            item.setLastUpdated(clock.now());
        }

        writer.createOrUpdate(item);
    }

    private void setUpdatedClips(List<Clip> clips, List<Clip> prevClips, DateTime now) {
        ImmutableMap<String, Clip> prevClipsMap = Maps.uniqueIndex(Iterables.filter(prevClips,
                HAS_CANONICAL_URI), TO_CANONICAL_URI);

        for (Clip clip: clips) {
            Clip prevClip = prevClipsMap.get(clip.getCanonicalUri());

            if (prevClip != null && equal(clip, prevClip) && prevClip.getLastUpdated() != null) {
                clip.setLastUpdated(prevClip.getLastUpdated());
            } else {
                clip.setLastUpdated(now);
            }
        }
    }

    private boolean equal(Clip clip, Clip prevClip) {
        return contentEqual(clip, prevClip)
                && Objects.equal(clip.getClipOf(), prevClip.getClipOf());
    }

    private void setUpdatedVersions(Set<Version> prevVersions, Set<Version> versions, DateTime now) {

        Map<String, Broadcast> prevBroadcasts = previousBroadcasts(prevVersions);
        Map<String, Location> prevLocations = previousLocations(prevVersions);

        for (Version version : versions) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                Broadcast prevBroadcast = prevBroadcasts.get(broadcast.getSourceId());
                if(prevBroadcast == null || !equal(prevBroadcast, broadcast)) {
                    broadcast.setLastUpdated(now);
                } else {
                    broadcast.setLastUpdated(prevBroadcast.getLastUpdated());
                }
            }
            for (Encoding encoding : version.getManifestedAs()) {
                for (Location location : encoding.getAvailableAt()) {
                    Location prevLocation = prevLocations.get(location.getUri());
                    if(prevLocation == null || !equal(prevLocation, location)) {
                        location.setLastUpdated(now);
                    } else {
                        location.setLastUpdated(prevLocation.getLastUpdated());
                    }
                }
            }
        }
    }

    private boolean equal(Location prevLocation, Location location) {
        return equal(prevLocation.getPolicy(), location.getPolicy())
                && Objects.equal(prevLocation.getAvailable(), location.getAvailable())
                && Objects.equal(prevLocation.getEmbedCode(), location.getEmbedCode())
                && Objects.equal(prevLocation.getEmbedId(), location.getEmbedId())
                && Objects.equal(prevLocation.getTransportIsLive(), location.getTransportIsLive())
                && Objects.equal(prevLocation.getTransportSubType(), location.getTransportSubType())
                && Objects.equal(prevLocation.getTransportType(), location.getTransportType())
                && Objects.equal(prevLocation.getAliases(), location.getAliases())
                && Objects.equal(prevLocation.getAliasUrls(), location.getAliasUrls())
                && Objects.equal(prevLocation.getAllUris(), location.getAllUris())
                ;
    }

    private boolean equal(Policy prevPolicy, Policy policy) {
        return Objects.equal(prevPolicy.getAvailabilityStart().toDateTime(DateTimeZone.UTC),
                policy.getAvailabilityStart().toDateTime(DateTimeZone.UTC))
                && Objects.equal(prevPolicy.getAvailabilityEnd().toDateTime(DateTimeZone.UTC), policy.getAvailabilityEnd().toDateTime(
                DateTimeZone.UTC))
                && Objects.equal(prevPolicy.getAvailableCountries(), policy.getAvailableCountries())
                && Objects.equal(prevPolicy.getActualAvailabilityStart(), policy.getActualAvailabilityStart())
                && Objects.equal(prevPolicy.getDrmPlayableFrom().toDateTime(DateTimeZone.UTC), policy.getDrmPlayableFrom().toDateTime(
                DateTimeZone.UTC))
                && Objects.equal(prevPolicy.getNetwork(), policy.getNetwork())
                && Objects.equal(prevPolicy.getPlatform(), policy.getPlatform())
                && Objects.equal(prevPolicy.getPlayer(), policy.getPlayer())
                && Objects.equal(prevPolicy.getPrice(), policy.getPrice())
                && Objects.equal(prevPolicy.getRevenueContract(), policy.getRevenueContract())
                && Objects.equal(prevPolicy.getService(), policy.getService())
                && Objects.equal(prevPolicy.getAliases(), policy.getAliases())
                && Objects.equal(prevPolicy.getAliasUrls(), policy.getAliasUrls())
                ;
    }

    private Map<String, Location> previousLocations(Set<Version> prevVersions) {
        return Maps.uniqueIndex(Iterables.concat(Iterables.transform(Iterables.concat(Iterables.transform(
                prevVersions,
                new Function<Version, Iterable<Encoding>>() {

                    @Override
                    public Iterable<Encoding> apply(Version input) {
                        return input.getManifestedAs();
                    }
                })), new Function<Encoding, Iterable<Location>>() {

            @Override
            public Iterable<Location> apply(Encoding input) {
                return input.getAvailableAt();
            }
        })), new Function<Location, String>() {

            @Override
            public String apply(Location input) {
                return input.getUri();
            }
        });
    }

    private boolean equal(Broadcast prevBroadcast, Broadcast broadcast) {
        return Objects.equal(prevBroadcast.getTransmissionTime().toDateTime(DateTimeZone.UTC),broadcast.getTransmissionTime().toDateTime(
                DateTimeZone.UTC))
                && Objects.equal(prevBroadcast.getTransmissionEndTime().toDateTime(DateTimeZone.UTC), broadcast.getTransmissionEndTime().toDateTime(
                DateTimeZone.UTC))
                && Objects.equal(prevBroadcast.getBroadcastDuration(), broadcast.getBroadcastDuration())
                && Objects.equal(prevBroadcast.isActivelyPublished(), broadcast.isActivelyPublished())
                && Objects.equal(prevBroadcast.getAudioDescribed(), broadcast.getAudioDescribed())
                && Objects.equal(prevBroadcast.getBlackoutRestriction(), broadcast.getBlackoutRestriction())
                && Objects.equal(prevBroadcast.getBroadcastOn(), broadcast.getBroadcastOn())
                && Objects.equal(prevBroadcast.getHighDefinition(), broadcast.getHighDefinition())
                && Objects.equal(prevBroadcast.getLive(), broadcast.getLive())
                && Objects.equal(prevBroadcast.getNewEpisode(), broadcast.getNewEpisode())
                && Objects.equal(prevBroadcast.getNewSeries(), broadcast.getNewSeries())
                && Objects.equal(prevBroadcast.getPremiere(), broadcast.getPremiere())
                && Objects.equal(prevBroadcast.getRepeat(), broadcast.getRepeat())
                && Objects.equal(prevBroadcast.getScheduleDate(), broadcast.getScheduleDate())
                && Objects.equal(prevBroadcast.getSigned(), broadcast.getSigned())
                && Objects.equal(prevBroadcast.getSourceId(), broadcast.getSourceId())
                && Objects.equal(prevBroadcast.getSubtitled(), broadcast.getSubtitled())
                && Objects.equal(prevBroadcast.getSurround(), broadcast.getSurround())
                && Objects.equal(prevBroadcast.getWidescreen(), broadcast.getWidescreen())
                && Objects.equal(prevBroadcast.getAliases(), broadcast.getAliases())
                && Objects.equal(prevBroadcast.getAliasUrls(), broadcast.getAliasUrls())
                ;
    }

    private ImmutableMap<String, Broadcast> previousBroadcasts(Set<Version> prevVersions) {
        Iterable<Broadcast> allBroadcasts = Iterables.concat(Iterables.transform(prevVersions, new Function<Version, Iterable<Broadcast>>() {
            @Override
            public Iterable<Broadcast> apply(Version input) {
                return input.getBroadcasts();
            }
        }));
        return Maps.uniqueIndex(allBroadcasts, new Function<Broadcast, String>() {

            @Override
            public String apply(Broadcast input) {
                return input.getSourceId();
            }
        });
    }

    private boolean itemsEqual(Item prevItem, Item item) {
        return contentEqual(prevItem, item)
                && Objects.equal(item.getPeople(), prevItem.getPeople())
                && Objects.equal(item.getBlackAndWhite(), prevItem.getBlackAndWhite())
                && Objects.equal(item.getContainer(), prevItem.getContainer())
                && Objects.equal(item.getCountriesOfOrigin(), prevItem.getCountriesOfOrigin())
                && Objects.equal(item.getIsLongForm(), prevItem.getIsLongForm())
                ;
    }

    private <T> boolean listsEqualNotCaringOrder(List<T> list1, List<T> list2) {
        if (list1.size() != list2.size()) {
            return false;
        }

        return Sets.newHashSet(list1).equals(Sets.newHashSet(list2));
    }

    @Override
    public void createOrUpdate(Container container) {
        Maybe<Identified> previously = resolver.findByCanonicalUris(ImmutableList.of(container.getCanonicalUri())).get(container.getCanonicalUri());

        if(previously.hasValue() && previously.requireValue() instanceof Container) {
            Container prevContainer = (Container) previously.requireValue();
            if(!equal(prevContainer, container)) {
                container.setLastUpdated(clock.now());
                container.setThisOrChildLastUpdated(clock.now());
            }
        }

        if(container.getLastUpdated() == null || previously.isNothing()) {
            container.setLastUpdated(clock.now());
            container.setThisOrChildLastUpdated(clock.now());
        }

        writer.createOrUpdate(container);
    }

    private boolean equal(Image image, Image prevImage) {
        return image.equals(prevImage)
                && Objects.equal(image.getAspectRatio(), prevImage.getAspectRatio())
                && Objects.equal(image.getAvailabilityEnd().toDateTime(DateTimeZone.UTC),
                prevImage.getAvailabilityEnd().toDateTime(DateTimeZone.UTC))
                && Objects.equal(image.getAvailabilityStart().toDateTime(DateTimeZone.UTC),
                prevImage.getAvailabilityStart().toDateTime(DateTimeZone.UTC))
                && Objects.equal(image.getColor(), prevImage.getColor())
                && Objects.equal(image.getHeight(), prevImage.getHeight())
                && Objects.equal(image.getMimeType(), prevImage.getMimeType())
                && Objects.equal(image.getTheme(), prevImage.getTheme())
                && Objects.equal(image.getType(), prevImage.getType())
                && Objects.equal(image.getWidth(), prevImage.getWidth())
                && Objects.equal(image.getAliases(), prevImage.getAliases())
                && Objects.equal(image.getAliasUrls(), prevImage.getAliasUrls())
                && Objects.equal(image.getAllUris(), prevImage.getAllUris())
                ;
    }

    private boolean equal(Container prevContainer, Container container) {
        return contentEqual(prevContainer, container);
    }

    private boolean contentEqual(Content prevContent, Content content) {
        return imagesEquals(prevContent.getImages(), content.getImages())
                && listsEqualNotCaringOrder(prevContent.getTopicRefs(), content.getTopicRefs())
                && Objects.equal(prevContent.getAliasUrls(), content.getAliasUrls())
                && Objects.equal(prevContent.getTitle(), content.getTitle())
                && Objects.equal(prevContent.getDescription(), content.getDescription())
                && Objects.equal(prevContent.getGenres(), content.getGenres())
                && Objects.equal(prevContent.getImage(), content.getImage())
                && Objects.equal(prevContent.getThumbnail(), content.getThumbnail())
                && Objects.equal(prevContent.getAliases(), content.getAliases())
                && Objects.equal(prevContent.getAllUris(), content.getAllUris())
                && Objects.equal(prevContent.getCertificates(), content.getCertificates())
                && Objects.equal(prevContent.getContentGroupRefs(), content.getContentGroupRefs())
                && Objects.equal(prevContent.getKeyPhrases(), content.getKeyPhrases())
                && Objects.equal(prevContent.getLanguages(), content.getLanguages())
                && Objects.equal(prevContent.getLongDescription(), content.getLongDescription())
                && Objects.equal(prevContent.getMediaType(), content.getMediaType())
                && Objects.equal(prevContent.getMediumDescription(), content.getMediumDescription())
                && Objects.equal(prevContent.getPresentationChannel(), content.getPresentationChannel())
                && Objects.equal(prevContent.getPrimaryImage(), content.getPrimaryImage())
                && Objects.equal(prevContent.getPublisher(), content.getPublisher())
                && Objects.equal(prevContent.getRelatedLinks(), content.getRelatedLinks())
                && Objects.equal(prevContent.getReviews(), content.getReviews())
                && Objects.equal(prevContent.getShortDescription(), content.getShortDescription())
                && Objects.equal(prevContent.getSpecialization(), content.getSpecialization())
                && Objects.equal(prevContent.getTags(), content.getTags())
                && Objects.equal(prevContent.getYear(), content.getYear())
                ;
    }

    private boolean imagesEquals(Set<Image> prevImages, Set<Image> images) {
        if (prevImages.size() != images.size()) {
            return false;
        }

        for (Image prevImage: prevImages) {
            if (contains(images, prevImage)) {
                return false;
            }
        }

        return true;
    }

    private boolean contains(Set<Image> images, Image prevImage) {
        for (Image image: images) {
            if (equal(image, prevImage)) {
                return true;
            }
        }

        return false;
    }

}
