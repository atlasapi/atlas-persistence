package org.uriplay.persistence.media.entity;

import org.uriplay.media.entity.Equiv;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class EquivTranslator {

    public Equiv fromDBObject(DBObject dbObject) {
        return new Equiv((String) dbObject.get("left"), (String) dbObject.get("right"));
    }
    
    public DBObject toDBObject(Equiv equiv) {
        DBObject dbObject = new BasicDBObject();
        dbObject.put(MongoConstants.ID, equiv.key());
        dbObject.put("left", equiv.left());
        dbObject.put("right", equiv.right());
        return dbObject;
    }
}
