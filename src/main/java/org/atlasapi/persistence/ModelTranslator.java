package org.atlasapi.persistence;

import com.mongodb.DBObject;

public interface ModelTranslator<T> {
    DBObject toDBObject(DBObject dbObject, T model);
    T fromDBObject(DBObject dbObject, T model);
}
