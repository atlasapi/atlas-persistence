package org.atlasapi.persistence.media.entity;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.BlackoutRestriction;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class BlackoutRestrictionTranslator {

    private static final String ALL_KEY = "all";
    
    public BlackoutRestriction fromDbObject(@Nullable DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        
        return new BlackoutRestriction(TranslatorUtils.toBoolean(dbo, ALL_KEY));
    }
    
    public DBObject toDbObject(@Nullable BlackoutRestriction blackoutRestriction) {
        if (blackoutRestriction == null) {
            return null;
        }
        
        DBObject dbObject = new BasicDBObject();
        TranslatorUtils.from(dbObject, ALL_KEY, blackoutRestriction.getAll());
        
        return dbObject;
    }
}
