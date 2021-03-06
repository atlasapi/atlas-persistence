package org.atlasapi.persistence.content.mongo;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.time.Clock;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.SeriesRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.ContentTranslator;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;
import org.atlasapi.persistence.player.PlayerResolver;
import org.atlasapi.persistence.service.ServiceResolver;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;

public class MongoContentWriter implements ContentWriter {

    private static final Set<String> DEFAULT_KEYS_TO_REMOVE = ImmutableSet.of(DescribedTranslator.LINKS_KEY);

    private static final Set<String> ALL_CONTENT_DB_KEYS = ImmutableSet.<String>builder()
            .addAll(ContainerTranslator.DB_KEYS)
            .addAll(ItemTranslator.DB_KEYS)
            .addAll(ContentTranslator.DB_KEYS)
            .addAll(DescribedTranslator.DB_KEYS)
            .addAll(IdentifiedTranslator.DB_KEYS)
            .build();

    // Excludes fields that should not be unset when updating a container such as those set by separate
    // denormalisation logic.
    private static final Set<String> ALL_BRAND_KEYS_TO_REMOVE = Sets.difference(
            ALL_CONTENT_DB_KEYS,
            ImmutableSet.of(
                    MongoConstants.ID,
                    ContainerTranslator.CHILDREN_KEY,
                    ContainerTranslator.FULL_SERIES_KEY
            )
    );

    // Excludes fields that should not be unset when updating a container such as those set by separate
    // denormalisation logic.
    private static final Set<String> ALL_SERIES_KEYS_TO_REMOVE = Sets.difference(
            ALL_CONTENT_DB_KEYS,
            ImmutableSet.of(
                    MongoConstants.ID,
                    ContainerTranslator.CHILDREN_KEY
            )
    );

    private final Logger log = LoggerFactory.getLogger(MongoContentWriter.class);
    private static final Logger timerLog = LoggerFactory.getLogger("TIMER");

    // We are only allowing this for certain publishers for now in case existing ingests are relying on
    // errors being thrown when hierarchy is missing.
    private static final Set<Publisher> CONTAINERLESS_EPISODE_ALLOWED_PUBLISHERS = ImmutableSet.of(
            Publisher.TESTING_MBST,
            Publisher.RADIO_TIMES_OVERRIDES
    );
    
    private final Clock clock;
    private final NewLookupWriter lookupStore;
    private final PlayerResolver playerResolver;
    private final ServiceResolver serviceResolver;

    private final ItemTranslator itemTranslator;
    private final ContainerTranslator containerTranslator;

    private final ChildRefWriter childRefWriter;

    private final DBCollection children;
    private final DBCollection topLevelItems;
    private final DBCollection containers;
    private final DBCollection programmeGroups;
    private final PersistenceAuditLog persistenceAuditLog;

    public MongoContentWriter(DatabasedMongo mongo, NewLookupWriter lookupStore,
            PersistenceAuditLog persistenceAuditLog, PlayerResolver playerResolver,
            ServiceResolver serviceResolver, Clock clock) {

        this.lookupStore = checkNotNull(lookupStore);
        this.clock = checkNotNull(clock);
        this.persistenceAuditLog = checkNotNull(persistenceAuditLog);
        this.playerResolver = checkNotNull(playerResolver);
        this.serviceResolver = checkNotNull(serviceResolver);

        MongoContentTables contentTables = new MongoContentTables(mongo);

        children = contentTables.collectionFor(ContentCategory.CHILD_ITEM);
        topLevelItems = contentTables.collectionFor(ContentCategory.TOP_LEVEL_ITEM);
        containers = contentTables.collectionFor(ContentCategory.CONTAINER);
        programmeGroups = contentTables.collectionFor(ContentCategory.PROGRAMME_GROUP);

        this.childRefWriter = new ChildRefWriter(mongo);
        NumberToShortStringCodec idCodec = new SubstitutionTableNumberCodec();
        this.itemTranslator = new ItemTranslator(idCodec);
        this.containerTranslator = new ContainerTranslator(idCodec);
    }

    protected MongoContentWriter(
            MongoContentWriter writer
    ) {
        this.clock = writer.clock;
        this.lookupStore = writer.lookupStore;
        this.playerResolver = writer.playerResolver;
        this.serviceResolver = writer.serviceResolver;
        this.itemTranslator = writer.itemTranslator;
        this.containerTranslator = writer.containerTranslator;
        this.childRefWriter = writer.childRefWriter;
        this.children = writer.children;
        this.topLevelItems = writer.topLevelItems;
        this.containers = writer.containers;
        this.programmeGroups = writer.programmeGroups;
        this.persistenceAuditLog = writer.persistenceAuditLog;
    }
    @Override
    public Item createOrUpdate(Item item) {
        checkNotNull(item, "Tried to persist null item");

        setThisOrChildLastUpdated(item);
        item.setLastFetched(clock.now());

        MongoQueryBuilder where = where().fieldEquals(IdentifiedTranslator.ID, item.getCanonicalUri());

        if (!item.hashChanged(itemTranslator.hashCodeOf(item))) {
            log.debug("Item {} hash not changed. Not writing.", item.getCanonicalUri());
            persistenceAuditLog.logNoWrite(item);
            return item;
        }

        validateRefs(item);

        persistenceAuditLog.logWrite(item);
        log.debug("Item {} hash changed so writing to db", item.getCanonicalUri());

        boolean itemOrParentlessEpisode = true;
        if (item instanceof Episode) {
            Episode episode = (Episode) item;
            boolean containerlessEpisodeAllowed = item.getPublisher() != null
                    && CONTAINERLESS_EPISODE_ALLOWED_PUBLISHERS.contains(item.getPublisher());
            if (item.getContainer() == null && !containerlessEpisodeAllowed) {
                throw new IllegalArgumentException(
                        String.format(
                                "Episodes from this publisher must have containers: Episode %s",
                                item.getCanonicalUri()
                        )
                );
            }

            if (episode.getSeriesRef() != null || episode.getContainer() != null) {
                itemOrParentlessEpisode = false;
                childRefWriter.includeEpisodeInSeriesAndBrand(episode);
                DBObject dbo = itemTranslator.toDB(item);
                children.update(where.build(), checkContainerRefs(dbo), UPSERT, SINGLE);

                remove(item.getCanonicalUri(), topLevelItems);
            }
        }
        if (itemOrParentlessEpisode) {
            if (item.getContainer() != null) {

                childRefWriter.includeItemInTopLevelContainer(item);
                DBObject dbo = itemTranslator.toDB(item);
                children.update(where.build(), checkContainerRefs(dbo), UPSERT, SINGLE);

                remove(item.getCanonicalUri(), topLevelItems);
            } else {
                topLevelItems.update(where.build(), itemTranslator.toDB(item), UPSERT, SINGLE);

                //disabled for now. need to remove the childref from the brand/series if enabled
                //remove(item.getCanonicalUri(), children);
            }
        }

        lookupStore.ensureLookup(item);

        return item;
    }

    /**
     * @deprecated use {@link #withContainerKeysToRemove(Iterable, Iterable)}
     *
     * This will return an inner class that will overwrite the list of keys to remove for containers when their
     * values are null. In MongoContentWriter if null values are passed in the CreateOrUpdate
     * method for Containers the keys are ignored rather than removed, and whatever value is in the db stays in
     * the db. At the point of creation of this method it is impossible to know if existing code
     * actually uses the logic to do merge updates. This results in the approach you see here, that
     * allows you to actually remove values without changing the legacy logic, while saving the need
     * to propagate the list through half the methods of this class.
     *
     * @param keysToRemove A list of DB keys to be removed if their values are null. Keys in {@link
     *                     MongoContentWriter#DEFAULT_KEYS_TO_REMOVE} will always be removed, and you do not
     *                     have to re-add them.
     * @return
     */
    @Deprecated
    public MongoContentWriter withKeysToRemove(Iterable<String> keysToRemove) {
        Iterable<String> modifiedKeysToRemove = Iterables.concat(DEFAULT_KEYS_TO_REMOVE, keysToRemove);
        return new InnerMongoContentWriter(modifiedKeysToRemove, modifiedKeysToRemove);
    }

    /**
     * This will return an inner class that will overwrite the list of keys to remove for containers when their values
     * are null. In the CreateOrUpdate method for Containers any null values for the Container's fields whose keys are
     * not provided here are ignored rather than removed, and whatever value is in the db stays in the db. Since legacy
     * logic meant most fields were ignored this method serves as a way of overriding default behaviour. Consider using
     * {@link #withAllContainerKeysToRemove()} if you want all appropriate fields to be updated.
     *
     * @param brandKeysToRemove  A list of DB keys to be removed for Brands if their values are null.
     * @param seriesKeysToRemove A list of DB keys to be removed for Series if their values are null.
     */
    public MongoContentWriter withContainerKeysToRemove(
            Iterable<String> brandKeysToRemove,
            Iterable<String> seriesKeysToRemove
    ) {
        return new InnerMongoContentWriter(brandKeysToRemove, seriesKeysToRemove);
    }

    /**
     * This will return an inner class that will overwrite the list of keys to remove for containers when their values
     * are null with a list of all container keys. In the CreateOrUpdate method for Containers any null values for the
     * Container's fields whose keys are not provided here are ignored rather than removed, and whatever value is in the
     * db stays in the db. Since legacy logic meant most fields were ignored this method serves as a way of overriding
     * default behaviour.
     */
    public MongoContentWriter withAllContainerKeysToRemove() {
        return new InnerMongoContentWriter(
                MongoContentWriter.ALL_BRAND_KEYS_TO_REMOVE,
                MongoContentWriter.ALL_SERIES_KEYS_TO_REMOVE
        );
    }

    private void validateRefs(Item item) {
        for (Location location : allLocations(item)) {

            Policy policy = location.getPolicy();
            if (policy != null) {
                if (policy.getService() != null) {
                    checkState(serviceResolver.serviceFor(policy.getService()).isPresent(),
                            "Service ID " + policy.getService() + " invalid");
                }
                if (policy.getPlayer() != null) {
                    checkState(playerResolver.playerFor(policy.getPlayer()).isPresent(),
                            "Player ID " + policy.getPlayer() + " invalid");
                }
            }
            ;
        }
    }

    private Iterable<Location> allLocations(Item item) {
        return concat(transform(allEncodings(item), Encoding.TO_LOCATIONS));
    }

    private Iterable<Encoding> allEncodings(Item item) {
        return concat(transform(item.getVersions(), Version.TO_ENCODINGS));
    }

    private DBObject checkContainerRefs(DBObject dbo) {
        checkContainerIdRef(dbo, ItemTranslator.CONTAINER, ItemTranslator.CONTAINER_ID);
        checkContainerIdRef(dbo, ItemTranslator.SERIES, ItemTranslator.SERIES_ID);
        return dbo;
    }

    private void checkContainerIdRef(DBObject dbo, String uriField, String idField) {
        if (dbo.containsField(uriField) && dbo.get(uriField) != null
            && (!dbo.containsField(idField) || dbo.get(idField) == null)) {
            log.warn("{} has {} {}, no id", new Object[]{
                    dbo.get(IdentifiedTranslator.ID), uriField, dbo.get(uriField)});
        }
    }

    private void remove(String canonicalUri, DBCollection containingCollection) {
        DBObject find = containingCollection.findOne(new BasicDBObject(MongoConstants.ID, canonicalUri), new BasicDBObject(ID, 1));
        if(find != null) {
            containingCollection.remove(new BasicDBObject(MongoConstants.ID, canonicalUri));
        }
    }

    @Override
    public void createOrUpdate(Container container) {
        checkNotNull(container);
        checkArgument(container instanceof Brand || container instanceof Series,
                "Not brand or series");

        Iterable<String> dbKeysToRemove = container instanceof Brand
                ? getBrandKeysToRemove()
                : getSeriesKeysToRemove();

        setThisOrChildLastUpdated(container);
        container.setLastFetched(clock.now());

        if (!container.hashChanged(containerTranslator.hashCodeOf(container))) {
            log.debug("Container {} hash not changed. Not writing.", container.getCanonicalUri());
            persistenceAuditLog.logNoWrite(container);
            return;
        }

        persistenceAuditLog.logWrite(container);
        log.debug("Container {} hash changed so writing to db. There are {} ChildRefs",
                container.getCanonicalUri(), container.getChildRefs().size());


        if (container instanceof Brand || isTopLevelSeries(container)) {

            DBObject containerDbo = containerTranslator.toDB(container);
            createOrUpdateContainer(container, containers, containerDbo, dbKeysToRemove);

            // The series inside a brand cannot be top level items any more so we
            // remove them as outer elements
            if (container instanceof Brand) {
                Brand brand = (Brand) container;

                Set<String> urisToRemove = Sets.newHashSet(Collections2.transform(brand.getSeriesRefs(), SeriesRef.TO_URI));
                if (!urisToRemove.isEmpty()) {
                    containers.remove(where().idIn(urisToRemove).build());
                }
            } else {
                createOrUpdateContainer(container, programmeGroups, containerDbo, dbKeysToRemove);
            }
        } else {
            Series series = (Series)container;
            childRefWriter.includeSeriesInTopLevelContainer(series);
            DBObject dbo = containerTranslator.toDB(container);
            checkContainerIdRef(dbo, ContainerTranslator.CONTAINER, ContainerTranslator.CONTAINER_ID);
            createOrUpdateContainer(container, programmeGroups, dbo, dbKeysToRemove);
            //this isn't a top-level series so ensure it's not in the container table.
            containers.remove(where().idEquals(series.getCanonicalUri()).build());
        }

    }

    private boolean isTopLevelSeries(Container container) {
        return container instanceof Series && ((Series)container).getParent() == null;
    }

    private void createOrUpdateContainer(
            Container container,
            DBCollection collection,
            DBObject containerDbo,
            Iterable<String> dbKeysToRemove
    ) {
        MongoQueryBuilder where = where().fieldEquals(IdentifiedTranslator.ID, container.getCanonicalUri());

        BasicDBObject op = set(containerDbo);
        unset(containerDbo, op, dbKeysToRemove);
        collection.update(where.build(), op, true, false);

        lookupStore.ensureLookup(container);
    }

    protected Iterable<String> getBrandKeysToRemove() {
        return DEFAULT_KEYS_TO_REMOVE;
    }

    protected Iterable<String> getSeriesKeysToRemove() {
        return DEFAULT_KEYS_TO_REMOVE;
    }

    /**
     * Since we do not perform a save() on containers, we must unset
     * keys that are not present in the object to be persisted. Since 
     * there is no prototype object from which to unset fields, we
     * maintain a list of keys to perform unsets on.
     */
    private void unset(DBObject dbo, BasicDBObject op, Iterable<String> keysToRemove) {
        BasicDBObject toRemove = new BasicDBObject();
        for (String key : keysToRemove) {
            if (!dbo.containsField(key)) {
                toRemove.put(key, 1);
            }
        }
        if (!toRemove.isEmpty()) {
            op.append(MongoConstants.UNSET, toRemove);
        }

    }

    private BasicDBObject set(DBObject dbo) {
        dbo.removeField(MongoConstants.ID);
        BasicDBObject containerUpdate = new BasicDBObject(MongoConstants.SET, dbo);

        return containerUpdate;
    }

    private void setThisOrChildLastUpdated(Item item) {
        DateTime thisOrChildLastUpdated = thisOrChildLastUpdated(null, item.getLastUpdated());

        for (Clip clip : item.getClips()) {
            thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, clip.getLastUpdated());
        }

        for (Version version : item.getVersions()) {
            thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, version.getLastUpdated());

            for (Broadcast broadcast : version.getBroadcasts()) {
                thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, broadcast.getLastUpdated());
            }

            for (Encoding encoding : version.getManifestedAs()) {
                thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, encoding.getLastUpdated());

                for (Location location : encoding.getAvailableAt()) {
                    thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, location.getLastUpdated());
                }
            }
        }
        item.setThisOrChildLastUpdated(thisOrChildLastUpdated);
    }

    private DateTime setThisOrChildLastUpdated(Container playlist) {
        DateTime thisOrChildLastUpdated = thisOrChildLastUpdated(null, playlist.getLastUpdated());
        for (ChildRef item : playlist.getChildRefs()) {
            DateTime itemOrChildUpdated = item.getUpdated();
            thisOrChildLastUpdated = thisOrChildLastUpdated(thisOrChildLastUpdated, itemOrChildUpdated);
        }
        playlist.setThisOrChildLastUpdated(thisOrChildLastUpdated);
        return thisOrChildLastUpdated;
    }

    private DateTime thisOrChildLastUpdated(DateTime current, DateTime candidate) {
        if (candidate != null && (current == null || candidate.isAfter(current))) {
            return candidate;
        }
        return current;
    }

    protected class InnerMongoContentWriter extends MongoContentWriter {

        private final Iterable<String> brandKeysToRemove;
        private final Iterable<String> seriesKeysToRemove;

        public InnerMongoContentWriter(Iterable<String> brandKeysToRemove, Iterable<String> seriesKeysToRemove) {
            super(MongoContentWriter.this);
            this.brandKeysToRemove = checkNotNull(brandKeysToRemove, "brandKeysToRemove");
            this.seriesKeysToRemove = checkNotNull(seriesKeysToRemove, "seriesKeysToRemove");
        }

        @Override
        protected Iterable<String> getBrandKeysToRemove() {
            return brandKeysToRemove;
        }

        @Override
        protected Iterable<String> getSeriesKeysToRemove() {
            return seriesKeysToRemove;
        }
    }
}
