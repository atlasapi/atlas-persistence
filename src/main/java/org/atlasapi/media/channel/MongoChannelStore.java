package org.atlasapi.media.channel;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.atlasapi.persistence.ids.MongoSequentialIdGenerator;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.persistence.mongo.MongoSortBuilder;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Equivalence;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;
import static org.atlasapi.media.channel.ChannelTranslator.ADVERTISE_FROM;
import static org.atlasapi.media.channel.ChannelTranslator.AVAILABLE_ON;
import static org.atlasapi.media.channel.ChannelTranslator.BROADCASTER;
import static org.atlasapi.media.channel.ChannelTranslator.CHANNEL_TYPE;
import static org.atlasapi.media.channel.ChannelTranslator.KEY;
import static org.atlasapi.media.channel.ChannelTranslator.MEDIA_TYPE;
import static org.atlasapi.media.channel.ChannelTranslator.PUBLISHER;
import static org.atlasapi.media.channel.ChannelTranslator.URI;
import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.CANONICAL_URL;
import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.IDS_NAMESPACE;
import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.IDS_VALUE;

public class MongoChannelStore implements ServiceChannelStore {

    public static final String COLLECTION = "channels";

    private static final Logger log = LoggerFactory.getLogger(MongoChannelStore.class);
    private static final ChannelTranslator translator = new ChannelTranslator();

    private static final String NUMBERING_CHANNEL_GROUP_ID = Joiner.on('.')
            .join(
                    ChannelTranslator.NUMBERINGS,
                    ChannelNumberingTranslator.CHANNEL_GROUP_KEY,
                    MongoConstants.ID
            );

    private static final Function<DBObject, Channel> DB_TO_CHANNEL_TRANSLATOR = input -> translator.fromDBObject(
            input,
            null
    );

    private final DBCollection collection;

    private final ChannelGroupResolver channelGroupResolver;
    private final ChannelGroupWriter channelGroupWriter;
    private final Equivalence<Channel> channelEquivalence;

    private MongoSequentialIdGenerator idGenerator;
    private SubstitutionTableNumberCodec codec;

    public MongoChannelStore(
            DatabasedMongo mongo,
            ChannelGroupResolver channelGroupResolver,
            ChannelGroupWriter channelGroupWriter
    ) {
        this(mongo, channelGroupResolver, channelGroupWriter, new DefaultEquivalence());
    }

    public MongoChannelStore(
            DatabasedMongo mongo,
            ChannelGroupResolver channelGroupResolver,
            ChannelGroupWriter channelGroupWriter,
            Equivalence<Channel> channelEquivalence
    ) {
        this.channelGroupResolver = channelGroupResolver;
        this.channelGroupWriter = channelGroupWriter;
        this.collection = mongo.collection(COLLECTION);
        this.idGenerator = new MongoSequentialIdGenerator(mongo, COLLECTION);
        this.codec = new SubstitutionTableNumberCodec();
        this.channelEquivalence = channelEquivalence;
    }

    @Override
    public Maybe<Channel> fromKey(final String key) {
        return Maybe.fromPossibleNullValue(
                translator.fromDBObject(
                        collection.findOne(where().fieldEquals(KEY, key).build()),
                        null
                )
        );
    }

    @Override
    public Maybe<Channel> fromId(long id) {
        return Maybe.fromPossibleNullValue(
                translator.fromDBObject(
                        collection.findOne(where().idEquals(id).build()),
                        null
                )
        );
    }

    @Override
    public Maybe<Channel> fromUri(final String uri) {
        return Maybe.fromPossibleNullValue(
                translator.fromDBObject(
                        collection.findOne(where().fieldEquals(CANONICAL_URL, uri).build()),
                        null
                )
        );
    }

    @Override
    public Iterable<Channel> forIds(Iterable<Long> ids) {
        return Iterables.transform(
                getOrderedCursor(where().longIdIn(ids).build()),
                DB_TO_CHANNEL_TRANSLATOR::apply
        );
    }

    @Override
    public Iterable<Channel> all() {
        return Iterables.transform(
                getOrderedCursor(new BasicDBObject()),
                DB_TO_CHANNEL_TRANSLATOR::apply
        );
    }

    @Override
    public Iterable<Channel> allChannels(ChannelQuery query) {
        MongoQueryBuilder mongoQuery = new MongoQueryBuilder();
        if (query.getBroadcaster().isPresent()) {
            mongoQuery.fieldEquals(BROADCASTER, query.getBroadcaster().get().key());
        }
        if (query.getMediaType().isPresent()) {
            mongoQuery.fieldEquals(MEDIA_TYPE, query.getMediaType().get().name());
        }
        if (query.getAvailableFrom().isPresent()) {
            mongoQuery.fieldEquals(AVAILABLE_ON, query.getAvailableFrom().get().key());
        }
        if (query.getChannelGroups().isPresent()) {
            mongoQuery.longFieldIn(NUMBERING_CHANNEL_GROUP_ID, query.getChannelGroups().get());
        }
        if (query.getGenres().isPresent()) {
            mongoQuery.fieldIn(ChannelTranslator.GENRES_KEY, query.getGenres().get());
        }
        if (query.getAdvertisedOn().isPresent()) {
            mongoQuery.fieldBeforeOrAt(ADVERTISE_FROM, query.getAdvertisedOn().get());
        }
        if (query.getPublisher().isPresent()) {
            mongoQuery.fieldEquals(PUBLISHER, query.getPublisher().get().key());
        }
        if (query.getUri().isPresent()) {
            mongoQuery.fieldEquals(URI, query.getUri().get());
        }
        if (query.getAliasNamespace().isPresent()) {
            mongoQuery.fieldEquals(IDS_NAMESPACE, query.getAliasNamespace().get());
        }
        if (query.getAliasValue().isPresent()) {
            mongoQuery.fieldEquals(IDS_VALUE, query.getAliasValue().get());
        }
        if (query.getChannelType().isPresent()) {
            mongoQuery.fieldEquals(CHANNEL_TYPE, query.getChannelType().get().name());
        }
        return Iterables.transform(
                getOrderedCursor(mongoQuery.build()),
                DB_TO_CHANNEL_TRANSLATOR::apply
        );
    }

    @Override
    public Maybe<Channel> forAlias(String alias) {
        MongoQueryBuilder query = new MongoQueryBuilder()
                .fieldEquals("aliases", alias);
        DBCursor cursor = getOrderedCursor(query.build());
        if (Iterables.isEmpty(cursor)) {
            return Maybe.nothing();
        }
        return Maybe.just(translator.fromDBObject(Iterables.getOnlyElement(cursor), null));
    }

    @Override
    public Map<String, Channel> forAliases(String aliasPrefix) {
        final Pattern prefixPattern = Pattern.compile(String.format(
                "^%s",
                Pattern.quote(aliasPrefix)
        ));

        Iterable<Channel> channels = all();

        Map<String, Channel> channelMap = Maps.newHashMap();
        for (Channel channel : channels) {
            for (String alias : Iterables.filter(
                    channel.getAliasUrls(),
                    Predicates.contains(prefixPattern)
            )) {
                if (channelMap.get(alias) == null) {
                    channelMap.put(alias, channel);
                } else {
                    log.error("duplicate alias " + alias + " on channels " + channelMap.get(alias)
                            .getId() + " & " + channel.getId());
                }
            }
        }
        return ImmutableMap.copyOf(channelMap);
    }

    // this method fetches channels by its aliases that are stored as ids in Mongo
    @Override
    public Iterable<Channel> forKeyPairAlias(ChannelQuery channelQuery) {
        MongoQueryBuilder queryBuilder = new MongoQueryBuilder();

        queryBuilder.fieldEquals(IDS_NAMESPACE, channelQuery.getAliasNamespace().get());
        queryBuilder.fieldEquals(IDS_VALUE, channelQuery.getAliasValue().get());

        return StreamSupport.stream(getOrderedCursor(queryBuilder.build()).spliterator(), false)
                .map(DB_TO_CHANNEL_TRANSLATOR)
                .collect(MoreCollectors.toImmutableList());
    }

    private DBCursor getOrderedCursor(DBObject query) {
        return collection.find(query)
                .sort(new MongoSortBuilder().ascending(MongoConstants.ID).build());
    }

    @Override
    public Channel createOrUpdate(Channel channel) {
        checkNotNull(channel);
        checkNotNull(channel.getUri());
        Maybe<Channel> existing = fromUri(channel.getUri());

        if (existing.hasValue()) {
            maintainParentLinks(channel, existing.requireValue());
        } else {
            channel.setId(codec.decode(idGenerator.generate()).longValue());
        }

        updateNumberingsOnChannelGroups(channel, existing);
        ensureParentReference(channel);
        setLastUpdated(channel, existing.valueOrNull(), DateTime.now(DateTimeZone.UTC));

        collection.update(
                new BasicDBObject(URI, channel.getUri()),
                translator.toDBObject(null, channel),
                UPSERT,
                SINGLE
        );

        return channel;
    }

    private void maintainParentLinks(Channel newChannel, Channel existingChannel) {
        if (existingChannel.getParent() != null) {
            if (newChannel.getParent() == null || !existingChannel.getParent()
                    .equals(newChannel.getParent())) {
                Maybe<Channel> maybeOldParent = fromId(existingChannel.getParent());
                Preconditions.checkState(
                        maybeOldParent.hasValue(),
                        String.format(
                                "Parent channel with id %s not found for channel with id %s",
                                newChannel.getParent(),
                                newChannel.getId()
                        )
                );

                Channel oldParent = maybeOldParent.requireValue();
                Set<Long> variations = Sets.newHashSet(oldParent.getVariations());
                variations.remove(existingChannel.getId());
                oldParent.setVariationIds(variations);
                collection.update(
                        new BasicDBObject(MongoConstants.ID, oldParent.getId()),
                        translator.toDBObject(null, oldParent),
                        UPSERT,
                        SINGLE
                );
            }
        }

        newChannel.setVariationIds(existingChannel.getVariations());

        SetView<ChannelNumbering> difference = Sets.difference(
                existingChannel.getChannelNumbers(),
                newChannel.getChannelNumbers()
        );
        if (!difference.isEmpty()) {
            for (ChannelNumbering oldNumbering : difference) {
                Optional<ChannelGroup> maybeGroup = channelGroupResolver.channelGroupFor(
                        oldNumbering.getChannelGroup());
                Preconditions.checkState(
                        maybeGroup.isPresent(),
                        String.format(
                                "ChannelGroup with id %s not found for channel with id %s",
                                oldNumbering.getChannelGroup(),
                                newChannel.getId()
                        )
                );
                ChannelGroup group = maybeGroup.get();

                Set<ChannelNumbering> numberings = Sets.newHashSet(group.getChannelNumberings());
                numberings.remove(oldNumbering);
                group.setChannelNumberings(numberings);

                channelGroupWriter.createOrUpdate(group);
            }
        }
    }

    private void updateNumberingsOnChannelGroups(Channel channel, Maybe<Channel> existingRecord) {

        if (existingRecord.hasValue()
                && channel.getChannelNumbers()
                .equals(existingRecord.requireValue().getChannelNumbers())) {
            return;
        }

        if (existingRecord.hasValue()) {
            removeLinks(
                    channel.getChannelNumbers(),
                    existingRecord.requireValue().getChannelNumbers()
            );
        }

        Multimap<Long, ChannelNumbering> newChannelGroupMapping = ArrayListMultimap.create();
        for (ChannelNumbering numbering : channel.getChannelNumbers()) {
            numbering.setChannel(channel.getId());
            newChannelGroupMapping.put(numbering.getChannelGroup(), numbering);
        }

        for (Long channelGroupId : newChannelGroupMapping.keySet()) {
            Optional<ChannelGroup> maybeGroup = channelGroupResolver.channelGroupFor(channelGroupId);
            Preconditions.checkState(
                    maybeGroup.isPresent(),
                    String.format(
                            "ChannelGroup with id %s not found for channel with id %s",
                            channelGroupId,
                            channel.getId()
                    )
            );
            ChannelGroup group = maybeGroup.get();
            for (ChannelNumbering numbering : newChannelGroupMapping.get(channelGroupId)) {
                group.addChannelNumbering(numbering);
            }
            channelGroupWriter.createOrUpdate(group);
        }
    }

    private void ensureParentReference(Channel channel) {
        if (channel.getParent() != null) {
            Maybe<Channel> maybeParent = fromId(channel.getParent());
            Preconditions.checkState(
                    maybeParent.hasValue(),
                    String.format(
                            "Parent channel with id %s not found for channel with id %s",
                            channel.getParent(),
                            channel.getId()
                    )
            );

            Channel parent = maybeParent.requireValue();
            parent.addVariation(channel.getId());
            collection.update(
                    new BasicDBObject(MongoConstants.ID, parent.getId()),
                    translator.toDBObject(null, parent),
                    UPSERT,
                    SINGLE
            );
        }
    }

    private void setLastUpdated(Channel current, @Nullable Channel previous, DateTime now) {
        if (previous == null
                || current.getLastUpdated() == null
                || !channelEquivalence.equivalent(current, previous)) {
            current.setLastUpdated(now);
        }
    }

    /**
     * Given a set of the new and existing ChannelNumberings, determines those numberings not in the
     * new set, and removes them from their channel group.
     *
     * @param newNumbers
     * @param existingNumbers
     */
    private void removeLinks(final Set<ChannelNumbering> newNumbers,
            Set<ChannelNumbering> existingNumbers) {
        SetView<ChannelNumbering> expiredNumberings = Sets.difference(existingNumbers, newNumbers);

        for (ChannelNumbering expired : expiredNumberings) {
            removeNumbering(expired);
        }
    }

    /**
     * Resolves the ChannelGroup for a given numbering, removes all numberings on that group with a
     * channel matching that of the provided numbering, then writes the group.
     *
     * @param expired
     */
    private void removeNumbering(final ChannelNumbering expired) {
        Optional<ChannelGroup> resolved = channelGroupResolver.channelGroupFor(expired.getChannelGroup());
        if (!resolved.isPresent()) {
            log.error(
                    "Tried to remove numbering {} from non-existent channel group {}",
                    expired,
                    expired.getChannelGroup()
            );
        }
        Iterable<ChannelNumbering> nonExpired = Iterables.filter(
                resolved.get().getChannelNumberings(),
                input -> !input.getChannel().equals(expired.getChannel())
        );
        resolved.get().setChannelNumberings(nonExpired);
        channelGroupWriter.createOrUpdate(resolved.get());
    }

    @Override
    public void start() {
        /* no-op */
    }

    @Override
    public void shutdown() {
        /* no-op */
    }

    private static class DefaultEquivalence extends Equivalence<Channel> {

        @Override
        protected boolean doEquivalent(@Nullable Channel a, @Nullable Channel b) {
            return a == b
                    || a != null
                    && b != null
                    // Identified
                    && Objects.equals(a.getId(), b.getId())
                    && Objects.equals(a.getCanonicalUri(), b.getCanonicalUri())
                    && Objects.equals(a.getCurie(), b.getCurie())
                    && Objects.equals(a.getAliasUrls(), b.getAliasUrls())
                    && Objects.equals(a.getAliases(), b.getAliases())
                    && Objects.equals(a.getEquivalentTo(), b.getEquivalentTo())
                    // Channel
                    && a.getSource() == b.getSource() && Objects.equals(a.getTitle(), b.getTitle())
                    && Objects.equals(a.getImages(), b.getImages())
                    && Objects.equals(a.getRelatedLinks(), b.getRelatedLinks())
                    && a.getMediaType() == b.getMediaType()
                    && Objects.equals(a.getKey(), b.getKey())
                    && Objects.equals(a.getHighDefinition(), b.getHighDefinition())
                    && Objects.equals(a.getRegional(), b.getRegional())
                    && Objects.equals(a.getAdult(), b.getAdult())
                    && Objects.equals(a.getTimeshift(), b.getTimeshift())
                    && Objects.equals(a.isTimeshifted(), b.isTimeshifted())
                    && a.getBroadcaster() == b.getBroadcaster()
                    && Objects.equals(a.getAdvertiseFrom(), b.getAdvertiseFrom())
                    && Objects.equals(a.getAvailableFrom(), b.getAvailableFrom())
                    && Objects.equals(a.getVariations(), b.getVariations())
                    && Objects.equals(a.getParent(), b.getParent())
                    && Objects.equals(a.getChannelNumbers(), b.getChannelNumbers())
                    && Objects.equals(a.getStartDate(), b.getStartDate())
                    && Objects.equals(a.getEndDate(), b.getEndDate())
                    && Objects.equals(a.getGenres(), b.getGenres())
                    && Objects.equals(a.getShortDescription(), b.getShortDescription())
                    && Objects.equals(a.getMediumDescription(), b.getMediumDescription())
                    && Objects.equals(a.getLongDescription(), b.getLongDescription())
                    && Objects.equals(a.getRegion(), b.getRegion())
                    && a.getChannelType() == b.getChannelType()
                    && Objects.equals(a.getTargetRegions(), b.getTargetRegions())
                    && Objects.equals(a.getInteractive(), b.getInteractive());
        }

        @Override
        protected int doHash(Channel channel) {
            return channel.hashCode();
        }
    }
}
