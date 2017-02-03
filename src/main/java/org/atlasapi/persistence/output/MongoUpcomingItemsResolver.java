package org.atlasapi.persistence.output;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.select;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.translator.TranslatorUtils.toDBObjectList;
import static com.metabroadcast.common.persistence.translator.TranslatorUtils.toDateTime;

import java.util.Collection;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.joda.time.DateTime;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultimap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.base.MorePredicates;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoUpcomingItemsResolver implements UpcomingItemsResolver {
    
    private final String versions = "versions";
    private final String broadcasts = "broadcasts";
    private final String transmissionEndTime = "transmissionEndTime";
    
    private final String transmissionEndTimeKey = Joiner.on(".").join(versions, broadcasts, transmissionEndTime);
    private final String containerKey = "container";
    
    private final DBObject fields = select().fields(ImmutableSet.of(IdentifiedTranslator.TYPE, 
            IdentifiedTranslator.OPAQUE_ID, DescribedTranslator.PUBLISHER_KEY, 
            IdentifiedTranslator.LAST_UPDATED, transmissionEndTimeKey))
            .build();

    private final DBCollection children;
    private final DBCollection topLevelItems;
    private final Clock clock;
    
    public MongoUpcomingItemsResolver(DatabasedMongo db) {
        this(db, new SystemClock());
    }
    
    public MongoUpcomingItemsResolver(DatabasedMongo db, Clock clock) {
        this.children = db.collection(ContentCategory.CHILD_ITEM.tableName());
        this.topLevelItems = db.collection(ContentCategory.TOP_LEVEL_ITEM.tableName());
        this.clock = clock;
    }
    
    @Override
    public Iterable<ChildRef> upcomingItemsFor(Container container) {
        return upcomingItemsByPublisherFor(container).values();
    }
    
    @Override
    public Multimap<Publisher, ChildRef> upcomingItemsByPublisherFor(Container container) {
        final DateTime now = clock.now();
        return filterToChildRef(now, broadcastEndsForChildrenOf(container, now));
    }
    
    @Override
    public Multimap<Publisher, ChildRef> upcomingItemsByPublisherFor(Item item, Application application) {
        final DateTime now = clock.now();
        return filterToChildRef(now, broadcastEndsForEquivalentTo(item, application));
    }
    
    private Iterable<DBObject> broadcastEndsForEquivalentTo(Item item, Application application) {
        final DateTime now = clock.now();
        DBObject query = where()
                .idIn(FluentIterable.from(item.getEquivalentTo())
                                    .filter(sourceFilter(application.getConfiguration().getEnabledReadSources()))
                                    .transform(LookupRef.TO_URI)
                     )
                .fieldAfter(transmissionEndTime, now)
                .build();
        return Iterables.concat(children.find(query,fields), topLevelItems.find(query,fields));
    }
    
    @Override
    public Iterable<ChildRef> upcomingItemsFor(Person person) {
        final DateTime now = clock.now();
        return filterToChildRef(now, broadcastEndsForItemsOf(person, now)).values();
    }

    private Multimap<Publisher, ChildRef> filterToChildRef(
            final DateTime now,
            Iterable<DBObject> broadcastEnds
    ) {
        
        Builder<Publisher, ChildRef> builder = ImmutableMultimap.builder();
        
        for (DBObject dbo : broadcastEnds) {
            for (DBObject version : toDBObjectList(dbo, versions)) {
                for (DBObject broadcast : toDBObjectList(version, broadcasts)) {
                    if (after(toDateTime(broadcast, transmissionEndTime), now)) {
                        String uri = TranslatorUtils.toString(dbo, MongoConstants.ID);
                        Long aid = TranslatorUtils.toLong(dbo, IdentifiedTranslator.OPAQUE_ID);
                        String type = TranslatorUtils.toString(dbo, IdentifiedTranslator.TYPE);
                        DateTime lastUpdated = TranslatorUtils.toDateTime(dbo, IdentifiedTranslator.LAST_UPDATED);
                        Publisher publisher = Publisher.fromKey(TranslatorUtils.toString(dbo, DescribedTranslator.PUBLISHER_KEY)).requireValue();
                        builder.put(publisher, new ChildRef(aid, uri, "", lastUpdated, EntityType.from(type)));
                    }
                }
            }
        }
        return builder.build();
    }
    
    private boolean after(DateTime dateTime, DateTime now) {
        return dateTime != null && dateTime.isAfter(now);
    }

    private Iterable<DBObject> broadcastEndsForChildrenOf(Container container, DateTime time) {
        DBObject query = where()
            .fieldEquals(containerKey, container.getCanonicalUri())
            .fieldAfter(transmissionEndTimeKey, time)
            .build();
        return children.find(query,fields);
    }

    private Iterable<DBObject> broadcastEndsForItemsOf(Person person, DateTime time) {
        DBObject query = where()
            .idIn(Iterables.transform(person.getContents(), ChildRef.TO_URI))
            .fieldAfter(transmissionEndTimeKey, time)
            .build();
        return Iterables.concat(children.find(query,fields), topLevelItems.find(query,fields));
    }
    
    private Predicate<LookupRef> sourceFilter(Collection<Publisher> sources) {
        return MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, Predicates.in(sources));
    }
    
}
