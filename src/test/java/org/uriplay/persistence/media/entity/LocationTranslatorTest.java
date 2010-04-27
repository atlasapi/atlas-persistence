package org.uriplay.persistence.media.entity;

import junit.framework.TestCase;

import org.joda.time.DateTime;
import org.uriplay.media.entity.Location;

import com.mongodb.DBObject;

public class LocationTranslatorTest extends TestCase {
    DescriptionTranslator dt = new DescriptionTranslator();
    LocationTranslator lt = new LocationTranslator(dt);
    
    public void testFromLocation() {
        Location location = new Location();
        location.setAvailable(true);
        location.setAvailabilityStart(new DateTime());
        location.setRestrictedBy("restrictied");
        
        DBObject dbObject = lt.toDBObject(null, location);
        
        assertEquals(location.getAvailable(), dbObject.get("available"));
        assertEquals(location.getAvailabilityStart().getMillis(), dbObject.get("availabilityStart"));
        assertEquals(location.getRestrictedBy(), dbObject.get("restrictedBy"));
    }
    
    public void testToLocation() {
        Location location = new Location();
        location.setAvailable(true);
        location.setAvailabilityStart(new DateTime());
        location.setAvailabilityEnd(new DateTime().plusHours(1));
        location.setRestrictedBy("restrictied");
        location.setDrmPlayableFrom(new DateTime());
        location.setEmbedCode("embed");
        location.setTransportSubType("sub");
        location.setTransportType("type");
        location.setTransportIsLive(true);
        location.setUri("uri");
        
        DBObject dbObject = lt.toDBObject(null, location);
        Location loc = lt.fromDBObject(dbObject, null);
        
        assertEquals(location.getAvailable(), loc.getAvailable());
        assertEquals(location.getAvailabilityStart(), loc.getAvailabilityStart());
        assertEquals(location.getAvailabilityEnd(), loc.getAvailabilityEnd());
        assertEquals(location.getRestrictedBy(), loc.getRestrictedBy());
        assertEquals(location.getDrmPlayableFrom(), loc.getDrmPlayableFrom());
        assertEquals(location.getEmbedCode(), loc.getEmbedCode());
        assertEquals(location.getTransportSubType(), loc.getTransportSubType());
        assertEquals(location.getTransportType(), loc.getTransportType());
        assertEquals(location.getTransportIsLive(), loc.getTransportIsLive());
        assertEquals(location.getUri(), loc.getUri());
    }
}
