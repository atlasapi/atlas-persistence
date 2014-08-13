package org.atlasapi.persistence.event;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.List;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Topic;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.mongodb.DBCollection;
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
    public Optional<Event> person(String uri) {
        Event event = translator.fromDBObject(collection.findOne(where().fieldEquals("uri", uri).build()));
        return Optional.fromNullable(event);
    }

    @Override
    public Optional<Event> fetch(Long id) {
        Event event = translator.fromDBObject(collection.findOne(where().idEquals(id).build()));
        return Optional.fromNullable(event);
    }

    @Override
    public Iterable<Event> fetchByEventGroup(Topic event) {
        // TODO Auto-generated method stub
        return null;
    }

}
