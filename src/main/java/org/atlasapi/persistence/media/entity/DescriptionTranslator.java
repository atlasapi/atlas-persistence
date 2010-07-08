package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Description;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class DescriptionTranslator implements ModelTranslator<Description> {
   
	public static final String ALIASES = "aliases";
	public static final String CANONICAL_URI = "canonicalUri";

    @Override
    public Description fromDBObject(DBObject dbObject, Description description) {
        if (description == null) {
            description = new Description();
        }

        description.setCanonicalUri((String) dbObject.get(CANONICAL_URI));
        description.setCurie((String) dbObject.get("curie"));
        description.setAliases(TranslatorUtils.toSet(dbObject, ALIASES));
        
        return description;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Description entity) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }

        TranslatorUtils.from(dbObject, CANONICAL_URI, entity.getCanonicalUri());
        TranslatorUtils.from(dbObject, "curie", entity.getCurie());
        TranslatorUtils.fromSet(dbObject, entity.getAliases(), ALIASES);

        return dbObject;
    }
}
