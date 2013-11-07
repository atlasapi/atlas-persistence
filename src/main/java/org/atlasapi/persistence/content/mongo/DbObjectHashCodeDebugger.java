package org.atlasapi.persistence.content.mongo;

import java.util.Map;

import org.slf4j.Logger;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.DBObject;


public class DbObjectHashCodeDebugger {

    @SuppressWarnings("unchecked")
    public void logHashCodes(DBObject dbObject, Logger log) {
        Object id = dbObject.get(MongoConstants.ID);
        log.trace("Logging hashes for {}", id);
        log.trace("Object ID [{}]: hashCode [{}]", dbObject.hashCode());
        for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) dbObject.toMap()).entrySet()) {
            Integer hashCode = entry.getValue() != null ? entry.getValue().hashCode() : null;
            log.trace("Object ID [{}]: Key [{}], hashCode [{}], Value: [{}]", 
                    new Object[] { id, entry.getKey(), hashCode, entry.getValue() });
        }
        log.trace("Done logging hashes for {}", id);
    }
}
