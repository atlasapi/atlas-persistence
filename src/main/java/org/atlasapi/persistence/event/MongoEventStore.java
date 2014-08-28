package org.atlasapi.persistence.event;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static org.atlasapi.persistence.event.EventTranslator.EVENT_GROUPS_KEY;
import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.CANONICAL_URL;
import static org.atlasapi.persistence.media.entity.IdentifiedTranslator.ID;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Topic;

import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoSortBuilder;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class MongoEventStore implements EventStore {

    private final DBCollection collection;
    private final EventTranslator translator = new EventTranslator();
    
    public MongoEventStore(DatabasedMongo db) {
        this.collection = checkNotNull(db).collection("events");
    }
    
    @Override
    public void createOrUpdate(Event event) {
        checkArgument(event.getId() != null);
        collection.save(translator.toDBObject(event));
    }

    @Override
    public Optional<Event> fetch(Long id) {
        Event event = translator.fromDBObject(collection.findOne(where().idEquals(id).build()));
        return Optional.fromNullable(event);
    }
    
    @Override
    public Optional<Event> fetch(String uri) {
        Event event = translator.fromDBObject(collection.findOne(where().fieldEquals(CANONICAL_URL, uri).build()));
        return Optional.fromNullable(event);
    }

    @Override
    public Iterable<Event> fetchByEventGroup(Topic eventGroup) {
        return Iterables.transform(
                getOrderedCursor(where().fieldEquals(EVENT_GROUPS_KEY + "." + ID, eventGroup.getId()).build()), 
                translator.translateDBObject()
        );
    }

    @Override
    public Iterable<Event> fetchAll() {
        return Iterables.transform(
                getOrderedCursor(new BasicDBObject()), 
                translator.translateDBObject()
        );
    }
    
    /**
     * orders cursor results by start date, ascending
     * @param query
     * @return
     */
    private DBCursor getOrderedCursor(DBObject query) {
        return collection.find(query).sort(new MongoSortBuilder().ascending(EventTranslator.START_TIME_KEY).build());
    }
}
