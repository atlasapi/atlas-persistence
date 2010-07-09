package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.TransportSubType;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class LocationTranslator implements ModelTranslator<Location> {
	
    private final DescriptionTranslator descriptionTranslator = new DescriptionTranslator(false);
	private final PolicyTranslator policyTranslator = new PolicyTranslator();
    
    @Override
    public Location fromDBObject(DBObject dbObject, Location entity) {
       
    	if (entity == null) {
            entity = new Location();
        }
        
        descriptionTranslator.fromDBObject(dbObject, entity);

        entity.setAvailable((Boolean) dbObject.get("available"));
        entity.setEmbedCode((String) dbObject.get("embedCode"));
        entity.setTransportIsLive((Boolean) dbObject.get("transportIsLive"));
        
        entity.setTransportType(readEnum(TransportType.class, dbObject, "transportType"));
        entity.setTransportSubType(readEnum(TransportSubType.class, dbObject, "transportSubType"));
        
        entity.setUri((String) dbObject.get("uri"));
        
        DBObject policyObject = (DBObject) dbObject.get("policy");
        if (policyObject != null) {
        	entity.setPolicy(policyTranslator.fromDBObject(policyObject, new Policy()));
        }
        return entity;
    }

    private <T extends Enum<T>> T readEnum(Class<T> clazz, DBObject dbObject, String field) {
    	String value = (String) dbObject.get(field);
    	if (value == null) { 
    		return null;
    	}
		return Enum.valueOf(clazz, value.toUpperCase());
	}

	@Override
    public DBObject toDBObject(DBObject dbObject, Location entity) {
        dbObject = descriptionTranslator.toDBObject(dbObject, entity);
        
        TranslatorUtils.from(dbObject, "available", entity.getAvailable());
        TranslatorUtils.from(dbObject, "embedCode", entity.getEmbedCode());
        TranslatorUtils.from(dbObject, "transportIsLive", entity.getTransportIsLive());
        
        if (entity.getTransportType() != null) {
        	TranslatorUtils.from(dbObject, "transportType", entity.getTransportType().toString());
        }
        
        if (entity.getTransportSubType() != null) {
        	TranslatorUtils.from(dbObject, "transportSubType", entity.getTransportSubType().toString());
        }
        
        TranslatorUtils.from(dbObject, "uri", entity.getUri());
        
        if (entity.getPolicy() != null) {
        	DBObject policyObject = policyTranslator.toDBObject(new BasicDBObject(), entity.getPolicy());
			if (!policyObject.keySet().isEmpty()) {
				dbObject.put("policy", policyObject);
			}
        }
        return dbObject;
    }
}
