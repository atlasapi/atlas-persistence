package org.atlasapi.persistence.content.mongo;

import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;

import com.google.common.collect.Ordering;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.DBObject;


public class DbObjectHashCodeDebugger {

    @SuppressWarnings("unchecked")
    public void logHashCodes(DBObject dbObject, Logger log) {
        Object id = dbObject.get(MongoConstants.ID);
        log.trace("Object ID [{}]: hashCode [{}]", id, dbObject.hashCode());
        Map<String, Object> dbMap = dbObject.toMap();
        for (String key : Ordering.natural().sortedCopy(dbMap.keySet())) {
            Object value = dbMap.get(key);
            Integer hashCode = value != null ? value.hashCode() : null;
            log.trace("Object ID [{}]: Key [{}], hashCode [{}], Value: [{}]", 
                    new Object[] { id, key, hashCode, value });
        }
        log.trace("Done logging hashes for {}", id);
    }
}
