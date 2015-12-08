package org.atlasapi.persistence.media.entity;


import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.EventRef;
import org.atlasapi.media.entity.Publisher;

public class EventRefTranslator {

    public static final String PUBLISHER = "publisher";

    public DBObject toDBObject(EventRef eventRef) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, MongoConstants.ID, eventRef.id());
        if(eventRef.getPublisher() != null) {
            TranslatorUtils.from(dbo, PUBLISHER, Publisher.TO_KEY.apply(eventRef.getPublisher()));
        }
        return dbo;
    }

    public EventRef fromDBObject(DBObject dbo) {
        return new EventRef(TranslatorUtils.toLong(dbo, MongoConstants.ID),
                Publisher.fromKey(TranslatorUtils.toString(dbo, PUBLISHER)).valueOrNull());
    }

    public Set<EventRef> fromDBObjects(Iterable<DBObject> dbos) {
        return ImmutableSet.copyOf(Iterables.transform(dbos, TO_EVENT_REF));
    }

    public BasicDBList toDBList(Iterable<EventRef> eventRefs) {
        BasicDBList list = new BasicDBList();
        list.addAll(ImmutableSet.copyOf(Iterables.transform(eventRefs, TO_DB_OBJECT)));
        return list;
    }

    private Function<DBObject, EventRef> TO_EVENT_REF = new Function<DBObject, EventRef>() {
        @Override
        public EventRef apply(DBObject dbObject) {
            return fromDBObject(dbObject);
        }
    };

    private Function<EventRef, DBObject> TO_DB_OBJECT = new Function<EventRef, DBObject>() {
        @Override
        public DBObject apply(@Nullable EventRef eventRef) {
            return toDBObject(eventRef);
        }
    };

}
