package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class DescriptionTranslator implements ModelTranslator<Identified> {
   
	public static final String CURIE = "curie";
	
	public static final String ALIASES = "aliases";
	public static final String LAST_UPDATED = "lastUpdated";
	public static final String EQUIVALENT_TO = "equivalent";
	public static final String ID = MongoConstants.ID;
	public static final String CANONICAL_URL = "uri";

	private boolean useAtlasIdAsId;
    
	public DescriptionTranslator() {
		this(false);
	}
	
	public DescriptionTranslator(boolean atlasIdAsId) {
		this.useAtlasIdAsId = atlasIdAsId;
	}
	
	@Override
    public Identified fromDBObject(DBObject dbObject, Identified description) {
        if (description == null) {
            description = new Identified();
        }
        
        if(useAtlasIdAsId) {
        	description.setCanonicalUri((String) dbObject.get(CANONICAL_URL));
        	description.setId((Long) dbObject.get(ID));
        }
        else {
        	description.setCanonicalUri((String) dbObject.get(ID));
        }
        
        description.setCurie((String) dbObject.get(CURIE));
        description.setAliases(TranslatorUtils.toSet(dbObject, ALIASES));
        description.setEquivalentTo(TranslatorUtils.toSet(dbObject, EQUIVALENT_TO));
        description.setLastUpdated(TranslatorUtils.toDateTime(dbObject, LAST_UPDATED));
        return description;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Identified entity) {
        if (dbObject == null) {
            dbObject = new BasicDBObject();
        }
        
        if(useAtlasIdAsId) {
        	TranslatorUtils.from(dbObject, CANONICAL_URL, entity.getCanonicalUri());
        	TranslatorUtils.from(dbObject, ID, entity.getId());
        }
        else {
        	TranslatorUtils.from(dbObject, ID, entity.getCanonicalUri());
        }
        
        TranslatorUtils.from(dbObject, CURIE, entity.getCurie());
        TranslatorUtils.fromSet(dbObject, entity.getAliases(), ALIASES);
        TranslatorUtils.fromSet(dbObject, entity.getEquivalentTo(), EQUIVALENT_TO);
        TranslatorUtils.fromDateTime(dbObject, LAST_UPDATED, entity.getLastUpdated());
        
        return dbObject;
    }
}
