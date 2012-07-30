package org.atlasapi.persistence.output;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.select;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.translator.TranslatorUtils.toDBObject;
import static com.metabroadcast.common.persistence.translator.TranslatorUtils.toDBObjectList;
import static com.metabroadcast.common.persistence.translator.TranslatorUtils.toDateTime;

import java.util.Comparator;

import org.atlasapi.media.entity.Container;
import org.atlasapi.persistence.content.ContentCategory;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

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

    private DBCollection children;
    private final Clock clock;
    
    public MongoAvailableChildrenResolver(DatabasedMongo db) {
        this(db, new SystemClock());
    }
    
    public MongoAvailableChildrenResolver(DatabasedMongo db, Clock clock) {
        this.children = db.collection(ContentCategory.CHILD_ITEM.tableName());
        this.clock = clock;
    }
    
    @Override
    public Iterable<String> availableChildrenFor(Container container) {
        final DateTime now = clock.now();
        return filterChildren(now, sortByAvailabilityStart(availablityWindowsForChildrenOf(container, now)));
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

    private Iterable<DBObject> availablityWindowsForChildrenOf(Container container, DateTime time) {
        DBObject query = where().fieldEquals(containerKey, container.getCanonicalUri()).fieldAfter(availabilityEndKey, time).build();
        return children.find(query,fields);
    }
    
    
}
