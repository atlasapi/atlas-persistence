package org.atlasapi.persistence.media.entity;

import java.util.Currency;

import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.Network;
import org.atlasapi.media.entity.Policy.Platform;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.persistence.media.ModelTranslator;

import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;

public class PolicyTranslator implements ModelTranslator<Policy> {
	
	@Override
    public Policy fromDBObject(DBObject dbObject, Policy entity) {
    	
        if (entity == null) {
            entity = new Policy();
         }
        if (dbObject.containsField("actualAvailabilityStart")) {
            entity.setAvailabilityStart(TranslatorUtils.toDateTime(dbObject, "actualAvailabilityStart"));
        }
        entity.setAvailabilityStart(TranslatorUtils.toDateTime(dbObject, "availabilityStart"));
        entity.setAvailabilityEnd(TranslatorUtils.toDateTime(dbObject, "availabilityEnd"));
        entity.setDrmPlayableFrom(TranslatorUtils.toDateTime(dbObject, "drmPlayableFrom"));
        
        String revenueContract = TranslatorUtils.toString(dbObject, "revenueContract");
        if (revenueContract != null) {
            entity.setRevenueContract(RevenueContract.fromKey(revenueContract));
        }
        if (dbObject.containsField("currency") && dbObject.containsField("price")) {
            entity.setPrice(new Price(Currency.getInstance(TranslatorUtils.toString(dbObject, "currency")), TranslatorUtils.toInteger(dbObject, "price")));
        }
        
        if (dbObject.containsField("availableCountries")) {
        	TranslatorUtils.toList(dbObject, "availableCountries");
        	entity.setAvailableCountries(Countries.fromCodes(TranslatorUtils.toList(dbObject, "availableCountries")));
        }
        
        if(dbObject.containsField("platform")) {
        	entity.setPlatform(Platform.fromKey(TranslatorUtils.toString(dbObject, "platform")));
        }
        if (dbObject.containsField("network")) {
            entity.setNetwork(Network.fromKey(TranslatorUtils.toString(dbObject, "network")));
        }
        
        return entity;
    }

	@Override
    public DBObject toDBObject(DBObject dbObject, Policy entity) {
        
	    if (entity.getActualAvailabilityStart() != null) {
	        TranslatorUtils.fromDateTime(dbObject, "actualAvailabilityStart", entity.getActualAvailabilityStart());
	    }
        TranslatorUtils.fromDateTime(dbObject, "availabilityStart", entity.getAvailabilityStart());
        TranslatorUtils.fromDateTime(dbObject, "availabilityEnd", entity.getAvailabilityEnd());
        TranslatorUtils.fromDateTime(dbObject, "drmPlayableFrom", entity.getDrmPlayableFrom());
        
        if (entity.getRevenueContract() != null) {
            TranslatorUtils.from(dbObject, "revenueContract", entity.getRevenueContract().key());
        }
        if (entity.getPrice() != null) {
            TranslatorUtils.from(dbObject, "currency", entity.getPrice().getCurrency().getCurrencyCode());
            TranslatorUtils.from(dbObject, "price", entity.getPrice().getAmount());
        }

        if (entity.getAvailableCountries() != null) {
        	TranslatorUtils.fromList(dbObject, Countries.toCodes(entity.getAvailableCountries()), "availableCountries");
        }
        if(entity.getPlatform() != null) {
        	TranslatorUtils.from(dbObject, "platform", entity.getPlatform().key());
        }
        if (entity.getNetwork().isPresent()) {
            TranslatorUtils.from(dbObject, "network", entity.getNetwork().get().key());
        }
        return dbObject;
    }
}
