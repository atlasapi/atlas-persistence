package org.uriplay.persistence.media.entity;

import org.uriplay.media.TransportType;
import org.uriplay.media.entity.Location;
import org.uriplay.media.entity.Policy;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class LocationTranslator implements DBObjectEntityTranslator<Location> {
	
    private final DescriptionTranslator descriptionTranslator;
	private final PolicyTranslator policyTranslator;
    
    public LocationTranslator(DescriptionTranslator descriptionTranslator, PolicyTranslator policyTranslator) {
        this.descriptionTranslator = descriptionTranslator;
		this.policyTranslator = policyTranslator;
    }

    @Override
    public Location fromDBObject(DBObject dbObject, Location entity) {
       
    	if (entity == null) {
            entity = new Location();
        }
        
        descriptionTranslator.fromDBObject(dbObject, entity);

        entity.setAvailable((Boolean) dbObject.get("available"));
        entity.setEmbedCode((String) dbObject.get("embedCode"));
        entity.setTransportIsLive((Boolean) dbObject.get("transportIsLive"));
        entity.setTransportSubType((String) dbObject.get("transportSubType"));
        entity.setTransportType(readTransportType(dbObject));
        entity.setUri((String) dbObject.get("uri"));
        
        DBObject policyObject = (DBObject) dbObject.get("policy");
        if (policyObject != null) {
        	entity.setPolicy(policyTranslator.fromDBObject(policyObject, new Policy()));
        }
        return entity;
    }

    private TransportType readTransportType(DBObject dbObject) {
    	String transportTypeString = (String) dbObject.get("transportType");
    	if (transportTypeString == null) { 
    		return null;
    	}
		return TransportType.fromString(transportTypeString);
	}

	@Override
    public DBObject toDBObject(DBObject dbObject, Location entity) {
        dbObject = descriptionTranslator.toDBObject(dbObject, entity);
        
        TranslatorUtils.from(dbObject, "available", entity.getAvailable());
        TranslatorUtils.from(dbObject, "embedCode", entity.getEmbedCode());
        TranslatorUtils.from(dbObject, "transportIsLive", entity.getTransportIsLive());
        TranslatorUtils.from(dbObject, "transportSubType", entity.getTransportSubType());
        
        if (entity.getTransportType() != null) {
        	TranslatorUtils.from(dbObject, "transportType", entity.getTransportType().toString());
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
