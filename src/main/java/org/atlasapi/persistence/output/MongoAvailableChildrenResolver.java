package org.atlasapi.persistence.output;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.select;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.translator.TranslatorUtils.toDBObject;
import static com.metabroadcast.common.persistence.translator.TranslatorUtils.toDBObjectList;
import static com.metabroadcast.common.persistence.translator.TranslatorUtils.toDateTime;

import java.util.Collection;
import java.util.Comparator;

import org.atlasapi.application.ApplicationConfiguration;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
 * Resolves URIs of available children of a given Container.
 * 
 * Queries Mongo for available children of a primary Container and its
 * equivalents (filtered by those available to application configuration).
 * 
 * The URIs are converted back to URIs of the primary Container's source via
 * resolution of their LookupEntries.
 * 
 */
public class MongoAvailableChildrenResolver implements AvailableChildrenResolver {

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
                        DBObject policy = toDBObject(location, MongoAvailableChildrenResolver.policy);
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
    
    private final DBObject fields = select().fields(availabilityStartKey, availabilityEndKey).build();

    private final LookupEntryStore entryStore;
    private final DBCollection children;
    private final Clock clock;

    
    public MongoAvailableChildrenResolver(DatabasedMongo db, LookupEntryStore entryStore) {
        this(db, entryStore, new SystemClock());
    }
    
    public MongoAvailableChildrenResolver(DatabasedMongo db, LookupEntryStore entryStore, Clock clock) {
        this.children = db.collection(ContentCategory.CHILD_ITEM.tableName());
        this.entryStore = checkNotNull(entryStore);
        this.clock = clock;
    }
    
    @Override
    public Iterable<String> availableChildrenFor(Container container, ApplicationConfiguration config) {
        final DateTime now = clock.now();
        return switchEquivs(filterChildren(now, sortByAvailabilityStart(availablityWindowsForChildrenOf(container, config, now))), container.getPublisher());
    }

    //switch URIs of available children to URIs of the containers source via their lookup refs.
    private Iterable<String> switchEquivs(Iterable<String> uris, final Publisher source) {
        Iterable<LookupEntry> entries = entryStore.entriesForCanonicalUris(uris);
        Iterable<LookupRef> refs = Iterables.concat(Iterables.transform(entries, LookupEntry.TO_EQUIVS));
        Iterable<LookupRef> sourceRefs = Iterables.filter(refs, sourceFilter(ImmutableSet.of(source)));
        return Iterables.transform(sourceRefs, LookupRef.TO_URI);
    }

    private Predicate<LookupRef> sourceFilter(Collection<Publisher> sources) {
        return MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, Predicates.in(sources));
    }

    private Iterable<DBObject> sortByAvailabilityStart(Iterable<DBObject> childDbos) {
        return AVAILABILITY_START_ORDERING.immutableSortedCopy(childDbos);
    }

    private Iterable<String> filterChildren(final DateTime now, Iterable<DBObject> availbleChildren) {
        return Iterables.filter(Iterables.transform(availbleChildren, new Function<DBObject, String>() {

            @Override
            public String apply(DBObject input) {
                for (DBObject version : toDBObjectList(input, versions)) {
                    for (DBObject encoding : toDBObjectList(version, encodings)) {
                        for (DBObject location : toDBObjectList(encoding, locations)) {
                            DBObject policy = toDBObject(location, MongoAvailableChildrenResolver.policy);
                            if (policy != null && before(toDateTime(policy, availabilityStart), now) && after(toDateTime(policy, availabilityEnd), now)) {
                                return TranslatorUtils.toString(input, MongoConstants.ID);
                            }
                        }
                    }
                }
                return null;
            }
        }),Predicates.notNull());
    }
    
    private static boolean before(DateTime dateTime, DateTime now) {
        return dateTime != null && dateTime.isBefore(now);
    }

    private boolean after(DateTime dateTime, DateTime now) {
        return dateTime != null && dateTime.isAfter(now);
    }

    // find available children for container and its equivalents from enabled sources
    private Iterable<DBObject> availablityWindowsForChildrenOf(Container container, ApplicationConfiguration config, DateTime time) {
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
    
    
}
