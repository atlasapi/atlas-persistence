package org.atlasapi.persistence.media.entity;

import java.util.Currency;
import java.util.Objects;

import junit.framework.TestCase;

import org.atlasapi.media.TransportSubType;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.Policy.RevenueContract;
import org.atlasapi.media.entity.Provider;
import org.atlasapi.media.entity.Quality;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class LocationTranslatorTest extends TestCase {
   
	private final LocationTranslator lt = new LocationTranslator();
    
    public void testFromLocation() {
        Location location = new Location();
        
        location.setAvailable(true);
        Provider provider = new Provider("provider_name", "provider-icon-url.co.uk");
        location.setProvider(provider);

        location.setPolicy(new Policy()
        					.withAvailabilityStart(new SystemClock().now())
        					.withAvailableCountries(Countries.IE, Countries.GB)
        					.withRevenueContract(RevenueContract.PAY_TO_BUY)
        					.withPrice(new Price(Currency.getInstance("GBP"), 199)));
        
        DBObject dbObject = lt.toDBObject(null, location);
        
        assertEquals(location.getAvailable(), dbObject.get("available"));
        DBObject providerObject = (DBObject) dbObject.get("provider");
        assertEquals(location.getProvider().getName(), providerObject.get("name"));
        assertEquals(location.getProvider().getIconUrl(), providerObject.get("iconUrl"));

        DBObject policyObject = (DBObject) dbObject.get("policy");
        assertEquals(Sets.newHashSet("GB", "IE"),  Sets.newHashSet(((BasicDBList)  policyObject.get("availableCountries"))));
        assertEquals(RevenueContract.PAY_TO_BUY.key(), policyObject.get("revenueContract"));
        assertEquals("GBP", policyObject.get("currency"));
        assertEquals(199, policyObject.get("price"));
    }
    
    public void testToLocation() {
        Location location = new Location();
        location.setAvailable(true);
        Provider provider = new Provider("provider_name", "provider-icon-url.co.uk");
        location.setProvider(provider);

        location.setPolicy(new Policy()
        	.withAvailabilityStart(new SystemClock().now())
        	.withAvailabilityEnd(new SystemClock().now().plusHours(1))
        	.withAvailableCountries(Countries.IE, Countries.GB)
        	.withDrmPlayableFrom(new SystemClock().now())
        	.withRevenueContract(RevenueContract.PAY_TO_RENT)
        	.withPrice(new Price(Currency.getInstance("USD"), 99)));

        location.setEmbedCode("embed");
        location.setEmbedId("embedId");
        location.setTransportSubType(TransportSubType.RTSP);
        location.setTransportType(TransportType.LINK);
        location.setTransportIsLive(true);
        location.setUri("uri");

        DBObject dbObject = lt.toDBObject(null, location);
        Location resultingLocation = lt.fromDBObject(dbObject, null);
        
        assertEquals(location.getAvailable(), resultingLocation.getAvailable());
        assertEquals(location.getProvider(), resultingLocation.getProvider());

        assertEquals(location.getPolicy().getAvailabilityStart(), resultingLocation.getPolicy().getAvailabilityStart());
        assertEquals(location.getPolicy().getAvailabilityEnd(), resultingLocation.getPolicy().getAvailabilityEnd());
        assertEquals(location.getPolicy().getDrmPlayableFrom(), resultingLocation.getPolicy().getDrmPlayableFrom());
        assertEquals(location.getPolicy().getRevenueContract(), resultingLocation.getPolicy().getRevenueContract());
        assertEquals(location.getPolicy().getPrice(), resultingLocation.getPolicy().getPrice());
        
        assertEquals(location.getEmbedCode(), resultingLocation.getEmbedCode());
        assertEquals(location.getEmbedId(), resultingLocation.getEmbedId());
        assertEquals(location.getTransportSubType(), resultingLocation.getTransportSubType());
        assertEquals(location.getTransportType(), resultingLocation.getTransportType());
        assertEquals(location.getTransportIsLive(), resultingLocation.getTransportIsLive());
        assertEquals(location.getUri(), resultingLocation.getUri());

    }
}
