package org.uriplay.persistence.media.entity;

import com.mongodb.DBObject;

public interface DBObjectEntityTranslator<T> {
    DBObject toDBObject(DBObject dbObject, T entity);
    T fromDBObject(DBObject dbObject, T entity);
}
