package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Description;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class DescriptionTranslator implements ModelTranslator<Description> {
   
	private static final String EMBEDDED_CANONICAL_URI = "canonicalUri";
	private static final String CURIE = "curie";
	private static final String LAST_UPDATED = "lastUpdated";
	public static final String ALIASES = "aliases";
	public static final String CANONICAL_URI = MongoConstants.ID;
	
	
	private final boolean useId;

    public DescriptionTranslator(boolean useId) {
		this.useId = useId;
	}
    
	@Override
    public Description fromDBObject(DBObject dbObject, Description description) {
        if (description == null) {
            description = new Description();
        }

        if (dbObject.containsField(CANONICAL_URI)) { 
        	description.setCanonicalUri((String) dbObject.get(CANONICAL_URI));
        } else {
        	description.setCanonicalUri((String) dbObject.get(EMBEDDED_CANONICAL_URI));
        }
        
        description.setCurie((String) dbObject.get(CURIE));
        description.setAliases(TranslatorUtils.toSet(dbObject, ALIASES));
        description.setLastUpdated(TranslatorUtils.toDateTime(dbObject, LAST_UPDATED));
        
        return description;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Description entity) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }
        
        if (useId) {
        	if (entity.getCanonicalUri() == null) {
        		throw new IllegalArgumentException("Cannot persist Content without a URI");
        	}
        	TranslatorUtils.from(dbObject, CANONICAL_URI, entity.getCanonicalUri());
        } else {
        	TranslatorUtils.from(dbObject, EMBEDDED_CANONICAL_URI, entity.getCanonicalUri());
        }

        TranslatorUtils.from(dbObject, CURIE, entity.getCurie());
        TranslatorUtils.fromSet(dbObject, entity.getAliases(), ALIASES);
        TranslatorUtils.fromDateTime(dbObject, LAST_UPDATED, entity.getLastUpdated());
        
        return dbObject;
    }
}
