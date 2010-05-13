package org.uriplay.persistence.media.entity;

import org.uriplay.media.entity.Countries;
import org.uriplay.media.entity.Policy;

import com.mongodb.DBObject;

public class PolicyTranslator implements DBObjectEntityTranslator<Policy> {
	
	@Override
    public Policy fromDBObject(DBObject dbObject, Policy entity) {
    	
        if (entity == null) {
            entity = new Policy();
        }
        entity.setAvailabilityStart(TranslatorUtils.toDateTime(dbObject, "availabilityStart"));
        entity.setAvailabilityEnd(TranslatorUtils.toDateTime(dbObject, "availabilityEnd"));
        entity.setDrmPlayableFrom(TranslatorUtils.toDateTime(dbObject, "drmPlayableFrom"));
        
        if (dbObject.containsField("availableCountries")) {
        	TranslatorUtils.toList(dbObject, "availableCountries");
        	entity.setAvailableCountries(Countries.fromCodes(TranslatorUtils.toList(dbObject, "availableCountries")));
        }
        
        return entity;
    }

	@Override
    public DBObject toDBObject(DBObject dbObject, Policy entity) {
        
        TranslatorUtils.fromDateTime(dbObject, "availabilityStart", entity.getAvailabilityStart());
        TranslatorUtils.fromDateTime(dbObject, "availabilityEnd", entity.getAvailabilityEnd());
        TranslatorUtils.fromDateTime(dbObject, "drmPlayableFrom", entity.getDrmPlayableFrom());

        if (entity.getAvailableCountries() != null) {
        	TranslatorUtils.fromList(dbObject, Countries.toCodes(entity.getAvailableCountries()), "availableCountries");
        }
        return dbObject;
    }
}
