package org.atlasapi.persistence.application;

import org.atlasapi.media.common.Id;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class MongoApplicationIdTranslator {

    public DBObject toDBObject(Id id) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, MongoConstants.ID, id.longValue());
        return dbo;
    }

    public Id fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        return Id.valueOf(TranslatorUtils.toLong(dbo, MongoConstants.ID));
    }
}
