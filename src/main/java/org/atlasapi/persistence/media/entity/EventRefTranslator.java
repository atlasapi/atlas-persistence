package org.atlasapi.persistence.media.entity;


import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
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

}
