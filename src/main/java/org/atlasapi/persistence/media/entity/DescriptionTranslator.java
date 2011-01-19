package org.atlasapi.persistence.media.entity;

import java.util.Set;

import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class DescriptionTranslator implements ModelTranslator<Identified> {
   
	private static final String EMBEDDED_CANONICAL_URI = "canonicalUri";
	private static final String CURIE = "curie";
	
	public static final String ALIASES = "aliases";
	public static final String LOOKUP = "lookup";
	public static final String LAST_UPDATED = "lastUpdated";
	public static final String EQUIVALENT_TO = "equivalent";
	public static final String CANONICAL_URI = MongoConstants.ID;
	
	private final boolean useId;

    public DescriptionTranslator(boolean useId) {
		this.useId = useId;
	}
    
	@Override
    public Identified fromDBObject(DBObject dbObject, Identified description) {
        if (description == null) {
            description = new Identified();
        }

        if (dbObject.containsField(CANONICAL_URI)) { 
        	description.setCanonicalUri((String) dbObject.get(CANONICAL_URI));
        } else {
        	description.setCanonicalUri((String) dbObject.get(EMBEDDED_CANONICAL_URI));
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
        TranslatorUtils.fromSet(dbObject, entity.getEquivalentTo(), EQUIVALENT_TO);
        TranslatorUtils.fromDateTime(dbObject, LAST_UPDATED, entity.getLastUpdated());
        
        // write a lookup field, used only for queries
        TranslatorUtils.fromSet(dbObject, lookupElemsFor(entity), LOOKUP);
        
        return dbObject;
    }

	public static Set<String> lookupElemsFor(Identified entity) {
		Set<String> lookupElems = Sets.newHashSet(entity.getCanonicalUri());
        if (entity.getCurie() != null) {
        	lookupElems.add(entity.getCurie());
        }
        lookupElems.addAll(entity.getAliases());
        return lookupElems;
	}
}
