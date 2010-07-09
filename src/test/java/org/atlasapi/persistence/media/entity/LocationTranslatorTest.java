package org.atlasapi.persistence.media.entity;

import junit.framework.TestCase;

import org.atlasapi.media.TransportSubType;
import org.atlasapi.media.TransportType;
import org.atlasapi.media.entity.Countries;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Policy;

import com.google.common.collect.Sets;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class LocationTranslatorTest extends TestCase {
   
	private final LocationTranslator lt = new LocationTranslator();
    
    public void testFromLocation() {
        Location location = new Location();
        
        location.setAvailable(true);
        
        location.setPolicy(new Policy()
        					.withAvailabilityStart(new SystemClock().now())
        					.withAvailableCountries(Countries.IE, Countries.GB));
        
        DBObject dbObject = lt.toDBObject(null, location);
        
        assertEquals(location.getAvailable(), dbObject.get("available"));
    
        DBObject policyObject = (DBObject) dbObject.get("policy");
        assertEquals(Sets.newHashSet("GB", "IE"),  Sets.newHashSet(((BasicDBList)  policyObject.get("availableCountries"))));
    }
    
    public void testToLocation() {
        Location location = new Location();
        location.setAvailable(true);
        
        
        location.setPolicy(new Policy()
        	.withAvailabilityStart(new SystemClock().now())
        	.withAvailabilityEnd(new SystemClock().now().plusHours(1))
        	.withAvailableCountries(Countries.IE, Countries.GB)
        	.withDrmPlayableFrom(new SystemClock().now()));

        location.setEmbedCode("embed");
        location.setTransportSubType(TransportSubType.RTSP);
        location.setTransportType(TransportType.LINK);
        location.setTransportIsLive(true);
        location.setUri("uri");
        
        DBObject dbObject = lt.toDBObject(null, location);
        Location resultingLocation = lt.fromDBObject(dbObject, null);
        
        assertEquals(location.getAvailable(), resultingLocation.getAvailable());

        assertEquals(location.getPolicy().getAvailabilityStart(), resultingLocation.getPolicy().getAvailabilityStart());
        assertEquals(location.getPolicy().getAvailabilityEnd(), resultingLocation.getPolicy().getAvailabilityEnd());
        assertEquals(location.getPolicy().getDrmPlayableFrom(), resultingLocation.getPolicy().getDrmPlayableFrom());
        
        assertEquals(location.getEmbedCode(), resultingLocation.getEmbedCode());
        assertEquals(location.getTransportSubType(), resultingLocation.getTransportSubType());
        assertEquals(location.getTransportType(), resultingLocation.getTransportType());
        assertEquals(location.getTransportIsLive(), resultingLocation.getTransportIsLive());
        assertEquals(location.getUri(), resultingLocation.getUri());
    }
}
