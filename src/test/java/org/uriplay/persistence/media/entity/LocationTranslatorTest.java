package org.uriplay.persistence.media.entity;

import junit.framework.TestCase;

import org.joda.time.DateTime;
import org.uriplay.media.TransportType;
import org.uriplay.media.entity.Countries;
import org.uriplay.media.entity.Location;
import org.uriplay.media.entity.Policy;

import com.google.common.collect.Sets;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public class LocationTranslatorTest extends TestCase {
    DescriptionTranslator dt = new DescriptionTranslator();
    LocationTranslator lt = new LocationTranslator(dt, new PolicyTranslator());
    
    public void testFromLocation() {
        Location location = new Location();
        
        location.setAvailable(true);
        
        location.setPolicy(new Policy()
        					.withAvailabilityStart(new DateTime())
        					.withAvailableCountries(Countries.IE, Countries.GB));
        
        DBObject dbObject = lt.toDBObject(null, location);
        
        assertEquals(location.getAvailable(), dbObject.get("available"));
    
        DBObject policyObject = (DBObject) dbObject.get("policy");
		assertEquals(location.getPolicy().getAvailabilityStart().getMillis(), policyObject.get("availabilityStart"));
        assertEquals(Sets.newHashSet("GB", "IE"),  Sets.newHashSet(((BasicDBList)  policyObject.get("availableCountries"))));
    }
    
    public void testToLocation() {
        Location location = new Location();
        location.setAvailable(true);
        
        
        location.setPolicy(new Policy()
        	.withAvailabilityStart(new DateTime())
        	.withAvailabilityEnd(new DateTime().plusHours(1))
        	.withAvailableCountries(Countries.IE, Countries.GB)
        	.withDrmPlayableFrom(new DateTime()));

        location.setEmbedCode("embed");
        location.setTransportSubType("sub");
        location.setTransportType(TransportType.HTMLEMBED);
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
