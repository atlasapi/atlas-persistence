package org.atlasapi.persistence.output;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.select;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Container;
import org.atlasapi.persistence.content.ContentCategory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoRecentlyBroadcastChildrenResolver implements RecentlyBroadcastChildrenResolver {

    private static final Function<DBObject, String> DBO_ID = new Function<DBObject, String>() {
        @Override
        public String apply(@Nullable DBObject input) {
            return (String) input.get(MongoConstants.ID);
        }
    };
    
    private static final String transmissionEndTimeKey = Joiner.on(".").join("versions", "broadcasts", "transmissionEndTime");
    private static final String containerKey = "container";
    
    private final DBObject select = select().field(MongoConstants.ID).build();
    private final DBObject sort = MongoBuilders.sort().descending(transmissionEndTimeKey).build();
    
    private final DBCollection collection;
    private final Clock clock;

    public MongoRecentlyBroadcastChildrenResolver(DatabasedMongo mongo) {
        this(mongo, new SystemClock());
    }
    
    public MongoRecentlyBroadcastChildrenResolver(DatabasedMongo mongo, Clock clock) {
        this.collection = mongo.collection(ContentCategory.CHILD_ITEM.tableName());
        this.clock = clock;
    }
    
    @Override
    public Iterable<String> recentlyBroadcastChildrenFor(Container container, int limit) {
        return Iterables.transform(recentlyBroadcast(container, limit), DBO_ID);
    }

    private Iterable<DBObject> recentlyBroadcast(Container container, int limit) {
        DBObject where = where()
                .fieldEquals(containerKey, container.getCanonicalUri())
                .fieldBeforeOrAt(transmissionEndTimeKey, clock.now())
                .build();
        return collection.find(where, select).sort(sort).limit(limit);
    }

}
