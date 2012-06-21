package org.atlasapi.persistence.event.mongo;

import org.atlasapi.messaging.event.EntityUpdatedEvent;
import org.joda.time.DateTime;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.atlasapi.persistence.event.RecentChangeStore;

public class MongoRecentChangesStore implements RecentChangeStore {

    private DBCollection changeCollection;
    
    private final Function<DBObject, EntityUpdatedEvent> FROM_DBOBJECT = 
        new Function<DBObject, EntityUpdatedEvent>() {
            @Override
            public EntityUpdatedEvent apply(DBObject input) {
                String changeId = TranslatorUtils.toString(input, "cid");
                DateTime timestamp = TranslatorUtils.toDateTime(input, "ts");
                String entityId = TranslatorUtils.toString(input, "eid");
                String entityType = TranslatorUtils.toString(input, "etype");
                String entitySource = TranslatorUtils.toString(input, "esource");
                return new EntityUpdatedEvent(changeId, timestamp, entityId, entityType, entitySource);
            }
        };

    public MongoRecentChangesStore(DatabasedMongo mongo) {
        changeCollection = mongo.createCollection("changes", 
            BasicDBObjectBuilder
                .start("capped", true)
                .add("max", 1000)
                .add("size", 100000)
                .get());
    }   

    @Override
    public void logChange(EntityUpdatedEvent event) {
        changeCollection.save(toDBObject(event));
    }

    private DBObject toDBObject(EntityUpdatedEvent event) {
        BasicDBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, "cid", event.getChangeId());
        TranslatorUtils.fromDateTime(dbo, "ts", event.getDateTime());
        TranslatorUtils.from(dbo, "eid", event.getEntityId());
        TranslatorUtils.from(dbo, "etype", event.getEntityType());
        TranslatorUtils.from(dbo, "esource", event.getEntitySource());
        return dbo;
    }

    @Override
    public Iterable<EntityUpdatedEvent> changes() {
        DBCursor changes = changeCollection.find().sort(new BasicDBObject(MongoConstants.NATURAL, -1));
        return Iterables.transform(changes, FROM_DBOBJECT);
    }

}
