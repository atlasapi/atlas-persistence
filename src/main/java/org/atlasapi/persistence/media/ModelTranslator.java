package org.atlasapi.persistence.media;

import com.mongodb.DBObject;

public interface ModelTranslator<T> {
    DBObject toDBObject(DBObject dbObject, T model);
    T fromDBObject(DBObject dbObject, T model);
}
