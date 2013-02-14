package org.atlasapi.persistence.media.entity;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Alias;
import org.atlasapi.media.entity.Broadcast;
import org.joda.time.Duration;
import org.joda.time.LocalDate;

import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBObject;

public class BroadcastTranslatorTest extends TestCase {
    BroadcastTranslator brt = new BroadcastTranslator();
    SystemClock clock = new SystemClock();
    
    public void testFromBroadcast() {
		Broadcast broadcast = new Broadcast("on", clock.now(), Duration.standardSeconds(1));
        
        DBObject dbObject = brt.toDBObject(broadcast);
        assertEquals(broadcast.getBroadcastDuration(), dbObject.get("broadcastDuration"));
    }
    
    public void testToBroadcast() {
		Broadcast broadcast = new Broadcast("on", clock.now(), Duration.standardSeconds(1));
        broadcast.setScheduleDate(new LocalDate(2010, 3, 20));
        broadcast.addAliasUrl("some alias");
        broadcast.addAlias(new Alias("some alias NS", "some alias value"));
        
        DBObject dbObject = brt.toDBObject(broadcast);
        Broadcast b = brt.fromDBObject(dbObject);
        
        assertEquals(broadcast.getScheduleDate(), b.getScheduleDate());
        assertEquals(broadcast.getTransmissionTime().getMillis(), b.getTransmissionTime().getMillis());
        assertEquals(broadcast.getBroadcastOn(), b.getBroadcastOn());
        assertEquals(broadcast.getScheduleDate(), b.getScheduleDate());
        assertFalse(b.getAliasUrls().isEmpty());
        assertFalse(b.getAliases().isEmpty());
    }
}
