package org.atlasapi.persistence.output;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.select;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.translator.TranslatorUtils.toDBObject;
import static com.metabroadcast.common.persistence.translator.TranslatorUtils.toDBObjectList;
import static com.metabroadcast.common.persistence.translator.TranslatorUtils.toDateTime;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.joda.time.DateTime;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.base.MorePredicates;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Resolves URIs of available items of a given Container.
 * 
 * Queries Mongo for available items of a primary Container and its
 * equivalents (filtered by those available to application configuration).
 * 
 * The URIs are converted back to URIs of the primary Container's source via
 * resolution of their LookupEntries.
 * 
 */
public class MongoAvailableItemsResolver implements AvailableItemsResolver {

    private static final Ordering<DBObject> AVAILABILITY_START_ORDERING = Ordering.from(new Comparator<DBObject>() {

        @Override
        public int compare(DBObject o1, DBObject o2) {
            DateTime one = earliestAvailabilityStart(o1);
            DateTime two = earliestAvailabilityStart(o2);
            
            return one.compareTo(two);
        }
        
        private DateTime earliestAvailabilityStart(DBObject dbo) {
            DateTime earliest = new DateTime(DateTimeZones.UTC);
            for (DBObject version : toDBObjectList(dbo, versions)) {
                for (DBObject encoding : toDBObjectList(version, encodings)) {
                    for (DBObject location : toDBObjectList(encoding, locations)) {
                        DBObject policy = toDBObject(location, MongoAvailableItemsResolver.policy);
                        if (policy != null) {
                            DateTime start = toDateTime(policy, availabilityStart);
                            if (before(start, earliest)) {
                                earliest = start;
                            }
                        }
                    }
                }
            }
            return earliest;
        }
        
    });
    
    private static final String versions = "versions";
    private static final String encodings = "manifestedAs";
    private static final String locations = "availableAt";
    private static final String policy = "policy";
    private static final String availabilityStart = "availabilityStart";
    private static final String availabilityEnd = "availabilityEnd";
    
    private final String availabilityStartKey = String.format("%s.%s.%s.%s.%s", versions, encodings, locations, policy, availabilityStart);
    private final String availabilityEndKey = String.format("%s.%s.%s.%s.%s", versions, encodings, locations, policy, availabilityEnd);
    private final String containerKey = "container";
    
    private final DBObject fields = select().fields(ImmutableSet.of(IdentifiedTranslator.TYPE, 
            IdentifiedTranslator.OPAQUE_ID, IdentifiedTranslator.LAST_UPDATED, 
            DescribedTranslator.PUBLISHER_KEY, availabilityStartKey, availabilityEndKey)).build();

    private final LookupEntryStore entryStore;
    private final DBCollection children;
    private final DBCollection topLevelItems;
    private final Clock clock;


    
    public MongoAvailableItemsResolver(DatabasedMongo db, LookupEntryStore entryStore) {
        this(db, entryStore, new SystemClock());
    }
    
    public MongoAvailableItemsResolver(DatabasedMongo db, LookupEntryStore entryStore, Clock clock) {
        this.children = db.collection(ContentCategory.CHILD_ITEM.tableName());
        this.topLevelItems = db.collection(ContentCategory.TOP_LEVEL_ITEM.tableName());
        this.entryStore = checkNotNull(entryStore);
        this.clock = clock;
    }
    
    @Override
    public Iterable<ChildRef> availableItemsFor(Container container, ApplicationConfiguration config) {
        return switchEquivs(availableItemsByPublisherFor(container, config).values(), container.getPublisher());
    }
    
    @Override
    public Multimap<Publisher, ChildRef> availableItemsByPublisherFor(Item item, ApplicationConfiguration config) {
        final DateTime now = clock.now();
        DBObject query = where()
                .idIn(FluentIterable.from(item.getEquivalentTo())
                                    .filter(sourceFilter(config.getEnabledSources()))
                                    .transform(LookupRef.TO_URI)
                     )
                .fieldAfter(availabilityEndKey, now)
                .build();
        return filterItems(now, Iterables.concat(children.find(query,fields), topLevelItems.find(query,fields)));
    }
    
    @Override
    public Multimap<Publisher, ChildRef> availableItemsByPublisherFor(Container container, ApplicationConfiguration config) {
        final DateTime now = clock.now();
        return filterItems(now, sortByAvailabilityStart(availablityWindowsForItemsOf(container, config, now)));
    }
    
    @Override
    /*
     * TODO: make this work better for Person from schedule-only sources.
     * 
     * At the moment we only look at the contents of the Person directly - for
     * more availability the LookupEntrys for the Person's contents should be
     * resolved and the entire set of all equivalents tested, switching back to
     * the Person's content identifiers for the final result.
     * 
     * So if a Person has Item I, resolve LE for I, and I -> (J, K), test
     * (I,J,K), J is available, return I.
     */
    public Iterable<ChildRef> availableItemsFor(Person person, ApplicationConfiguration config) {
        final DateTime now = clock.now();
        return switchEquivs(filterItems(now, sortByAvailabilityStart(availablityWindowsForItemsOf(person, now))).values(), person.getPublisher());
    }

    //switch URIs of available items to URIs of the containers source via their lookup refs.
    private Iterable<ChildRef> switchEquivs(Iterable<ChildRef> childRefs, final Publisher source) {
        Map<String, LookupEntry> entries = Maps.uniqueIndex(entryStore.entriesForCanonicalUris(
                Iterables.transform(childRefs, ChildRef.TO_URI)), LookupEntry.TO_ID);

        Builder<ChildRef> rewrittenRefs = ImmutableSet.builder();
        Predicate<LookupRef> sourceFilter = sourceFilter(ImmutableSet.of(source));
        for (ChildRef childRef : childRefs) {
            Iterable<LookupRef> sourceRefs = Iterables.filter(entries.get(childRef.getUri()).equivalents(), sourceFilter);
            if (Iterables.size(sourceRefs) > 0) {
                LookupRef lookupRef = sourceRefs.iterator().next();
                rewrittenRefs.add(new ChildRef(lookupRef.id(), 
                                    lookupRef.uri(), 
                                    childRef.getSortKey(), 
                                    childRef.getUpdated(), 
                                    childRef.getType()
                                 ));
            } else {
                // There is no equivalent item in the target source; ignore this reference
            }
            
        }
        return rewrittenRefs.build();
    }

    private Predicate<LookupRef> sourceFilter(Collection<Publisher> sources) {
        return MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, Predicates.in(sources));
    }

    private Iterable<DBObject> sortByAvailabilityStart(Iterable<DBObject> childDbos) {
        return AVAILABILITY_START_ORDERING.immutableSortedCopy(childDbos);
    }

    private Multimap<Publisher, ChildRef> filterItems(final DateTime now, Iterable<DBObject> availableItems) {
        
        ImmutableMultimap.Builder<Publisher, ChildRef> itemMap = ImmutableMultimap.builder();
        for (DBObject availableItem : availableItems) {
            for (DBObject version : toDBObjectList(availableItem, versions)) {
                for (DBObject encoding : toDBObjectList(version, encodings)) {
                    for (DBObject location : toDBObjectList(encoding, locations)) {
                        DBObject policy = toDBObject(location, MongoAvailableItemsResolver.policy);
                        if (policy != null && before(toDateTime(policy, availabilityStart), now) && after(toDateTime(policy, availabilityEnd), now)) {
                            String uri = TranslatorUtils.toString(availableItem, MongoConstants.ID);
                            Long aid = TranslatorUtils.toLong(availableItem, IdentifiedTranslator.OPAQUE_ID);
                            String type = TranslatorUtils.toString(availableItem, IdentifiedTranslator.TYPE);
                            DateTime lastUpdated = TranslatorUtils.toDateTime(availableItem, IdentifiedTranslator.LAST_UPDATED);
                            Publisher publisher = Publisher.fromKey(TranslatorUtils.toString(availableItem, 
                                                                    DescribedTranslator.PUBLISHER_KEY)
                                                                   ).requireValue();
                            itemMap.put(publisher, 
                                        new ChildRef(aid, uri, "", lastUpdated, EntityType.from(type)));
                        }
                    }
                }
            }
        }
        return itemMap.build();
    }
    
    private static boolean before(DateTime dateTime, DateTime now) {
        return dateTime != null && dateTime.isBefore(now);
    }

    private boolean after(DateTime dateTime, DateTime now) {
        return dateTime != null && dateTime.isAfter(now);
    }

    // find available items for container and its equivalents from enabled sources
    private Iterable<DBObject> availablityWindowsForItemsOf(Container container, ApplicationConfiguration config, DateTime time) {
        DBObject query = where()
            .fieldIn(containerKey, containerAndEquivalents(container, config))
            .fieldAfter(availabilityEndKey, time)
            .build();
        return children.find(query,fields);
    }

    private Iterable<String> containerAndEquivalents(Container container, final ApplicationConfiguration config) {
        return Iterables.concat(ImmutableSet.of(container.getCanonicalUri()),
                Iterables.transform(Iterables.filter(
                        container.getEquivalentTo(), sourceFilter(config.getEnabledSources())), 
                LookupRef.TO_URI));
    }

    // find available items for container and its equivalents from enabled sources
    private Iterable<DBObject> availablityWindowsForItemsOf(Person person, DateTime time) {
        DBObject query = where()
                .idIn(Iterables.transform(person.getContents(), ChildRef.TO_URI))
                .fieldAfter(availabilityEndKey, time)
                .build();
        return Iterables.concat(children.find(query,fields), topLevelItems.find(query,fields));
    }
    
}
