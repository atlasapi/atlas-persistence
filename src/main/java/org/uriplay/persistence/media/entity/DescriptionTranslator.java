package org.uriplay.persistence.media.entity;

import org.uriplay.media.entity.Description;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class DescriptionTranslator implements DBObjectEntityTranslator<Description> {
    public static final String CANONICAL_URI = "canonicalUri";

    @Override
    public Description fromDBObject(DBObject dbObject, Description description) {
        if (description == null) {
            description = new Description();
        }

        description.setCanonicalUri((String) dbObject.get(CANONICAL_URI));
        description.setCurie((String) dbObject.get("curie"));
        description.setAliases(TranslatorUtils.toSet(dbObject, "aliases"));
        
        return description;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Description entity) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }

        TranslatorUtils.from(dbObject, CANONICAL_URI, entity.getCanonicalUri());
        TranslatorUtils.from(dbObject, "curie", entity.getCurie());
        TranslatorUtils.fromSet(dbObject, entity.getAliases(), "aliases");

        return dbObject;
    }
}
