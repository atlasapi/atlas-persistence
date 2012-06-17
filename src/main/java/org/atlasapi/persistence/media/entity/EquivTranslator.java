package org.atlasapi.persistence.media.entity;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.Set;

import org.atlasapi.media.content.Equiv;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class EquivTranslator {

    private static final String RIGHT_KEY = "right";
	private static final String LEFT_KEY = "left";

	public Equiv fromDBObject(DBObject dbObject) {
        return new Equiv((String) dbObject.get(LEFT_KEY), (String) dbObject.get(RIGHT_KEY));
    }
	
	public Iterable<Equiv> fromDBObject(Iterable<DBObject> dbObjects) {
       return Iterables.transform(dbObjects, new Function<DBObject, Equiv>() {
			@Override
			public Equiv apply(DBObject dbObject) {
				return fromDBObject(dbObject);
			}
       });
    }
    
    public DBObject toDBObject(Equiv equiv) {
        DBObject dbObject = new BasicDBObject();
        dbObject.put(MongoConstants.ID, equiv.key());
        dbObject.put(LEFT_KEY, equiv.left());
        dbObject.put(RIGHT_KEY, equiv.right());
        return dbObject;
    }
    
    public MongoQueryBuilder findPathLengthOne(Set<String> ids) {
    	return where().or(where().fieldIn(LEFT_KEY, ids), where().fieldIn(RIGHT_KEY, ids));
    }
}
