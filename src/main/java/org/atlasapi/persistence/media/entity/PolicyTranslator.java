package org.atlasapi.persistence.media.entity;

import java.util.Currency;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.Network;
import org.atlasapi.media.entity.Policy.Platform;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.media.entity.simple.Pricing;
import org.atlasapi.persistence.ApiContentFields;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;
import org.atlasapi.persistence.PricingTranslator;

public class PolicyTranslator implements ModelTranslator<Policy> {

    private static final String PLAYER_KEY = "player";
    private static final String SERVICE_KEY = "service";
    private static final String TERMS_OF_USE_KEY = "termsOfUse";
    private static final String PRICING_KEY = "pricing";
    private static final String SUBSCRIPTION_PACKAGES = "subscriptionPackages";
    
    private final PricingTranslator pricingTranslator = new PricingTranslator();

    @Override
    public Policy fromDBObject(DBObject dbObject, Policy entity) {
    	
        if (entity == null) {
            entity = new Policy();
         }
        entity.setActualAvailabilityStart(TranslatorUtils.toDateTime(dbObject, "actualAvailabilityStart"));
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
        if (dbObject.containsField(SUBSCRIPTION_PACKAGES)) {
            entity.setSubscriptionPackages(TranslatorUtils.toSet(dbObject, SUBSCRIPTION_PACKAGES));
        }
        
        entity.setService(TranslatorUtils.toLong(dbObject, SERVICE_KEY));
        entity.setPlayer(TranslatorUtils.toLong(dbObject, PLAYER_KEY));
        entity.setTermsOfUse(TranslatorUtils.toString(dbObject, TERMS_OF_USE_KEY));
        if (dbObject.containsField(PRICING_KEY)) {
            entity.setPricing(Iterables.transform((Iterable<DBObject>) dbObject.get(PRICING_KEY), new Function<DBObject, Pricing>() {

                @Override
                public Pricing apply(DBObject input) {
                    return pricingTranslator.fromDBObject(input, null);
                }
            }));
        }
        return entity;
    }

    @Override
    public DBObject unsetFields(DBObject dbObject, Iterable<ApiContentFields> fieldsToUnset) {
        return dbObject;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Policy entity) {
        
	    TranslatorUtils.fromDateTime(dbObject, "actualAvailabilityStart", entity.getActualAvailabilityStart());
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
        if (entity.getNetwork() != null) {
            TranslatorUtils.from(dbObject, "network", entity.getNetwork().key());
        }
        if (entity.getService() != null) {
            TranslatorUtils.from(dbObject, SERVICE_KEY, entity.getService());
        }
        if (entity.getPlayer() != null) {
            TranslatorUtils.from(dbObject, PLAYER_KEY, entity.getPlayer());
        }
        if(entity.getTermsOfUse() != null) {
            TranslatorUtils.from(dbObject, TERMS_OF_USE_KEY, entity.getTermsOfUse());
        }

        BasicDBList pricingDbObject = new BasicDBList();
        for (Pricing pricing : entity.getPricing()) {
            pricingDbObject.add(pricingTranslator.toDBObject(new BasicDBObject(), pricing));
        }

        TranslatorUtils.fromIterable(dbObject, entity.getSubscriptionPackages(), SUBSCRIPTION_PACKAGES);
        dbObject.put(PRICING_KEY, pricingDbObject);
        return dbObject;
    }
}
