package org.uriplay.persistence.media.entity;

import org.uriplay.media.TransportType;
import org.uriplay.media.entity.Location;

import com.mongodb.DBObject;

public class LocationTranslator implements DBObjectEntityTranslator<Location> {
    private final DescriptionTranslator descriptionTranslator;
    
    public LocationTranslator(DescriptionTranslator descriptionTranslator) {
        this.descriptionTranslator = descriptionTranslator;
    }

    @Override
    public Location fromDBObject(DBObject dbObject, Location entity) {
        if (entity == null) {
            entity = new Location();
        }
        
        descriptionTranslator.fromDBObject(dbObject, entity);
        
        entity.setAvailabilityStart(TranslatorUtils.toDateTime(dbObject, "availabilityStart"));
        entity.setAvailabilityEnd(TranslatorUtils.toDateTime(dbObject, "availabilityEnd"));
        entity.setAvailable((Boolean) dbObject.get("available"));
        entity.setDrmPlayableFrom(TranslatorUtils.toDateTime(dbObject, "drmPlayableFrom"));
        entity.setEmbedCode((String) dbObject.get("embedCode"));
        entity.setRestrictedBy((String) dbObject.get("restrictedBy"));
        entity.setTransportIsLive((Boolean) dbObject.get("transportIsLive"));
        entity.setTransportSubType((String) dbObject.get("transportSubType"));
        entity.setTransportType(readTransportType(dbObject));
        entity.setUri((String) dbObject.get("uri"));
        
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
        
        TranslatorUtils.fromDateTime(dbObject, "availabilityStart", entity.getAvailabilityStart());
        TranslatorUtils.fromDateTime(dbObject, "availabilityEnd", entity.getAvailabilityEnd());
        TranslatorUtils.from(dbObject, "available", entity.getAvailable());
        TranslatorUtils.fromDateTime(dbObject, "drmPlayableFrom", entity.getDrmPlayableFrom());
        TranslatorUtils.from(dbObject, "embedCode", entity.getEmbedCode());
        TranslatorUtils.from(dbObject, "restrictedBy", entity.getRestrictedBy());
        TranslatorUtils.from(dbObject, "transportIsLive", entity.getTransportIsLive());
        TranslatorUtils.from(dbObject, "transportSubType", entity.getTransportSubType());
        if (entity.getTransportType() != null) {
        	TranslatorUtils.from(dbObject, "transportType", entity.getTransportType().toString());
        }
        TranslatorUtils.from(dbObject, "uri", entity.getUri());
        
        return dbObject;
    }

}
