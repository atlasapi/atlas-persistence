package org.uriplay.persistence.media.entity;

import junit.framework.TestCase;

import org.joda.time.LocalDate;
import org.uriplay.media.entity.Broadcast;

import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBObject;

public class BroadcastTranslatorTest extends TestCase {
    DescriptionTranslator dt = new DescriptionTranslator();
    BroadcastTranslator brt = new BroadcastTranslator(dt);
    
    public void testFromBroadcast() {
        Broadcast broadcast = new Broadcast();
        broadcast.setBroadcastDuration(1);
        broadcast.setTransmissionTime(new SystemClock().now());
        
        DBObject dbObject = brt.toDBObject(null, broadcast);
        assertEquals(broadcast.getBroadcastDuration(), dbObject.get("broadcastDuration"));
    }
    
    public void testToBroadcast() {
        Broadcast broadcast = new Broadcast();
        broadcast.setBroadcastDuration(1);
        broadcast.setTransmissionTime(new SystemClock().now());
        broadcast.setBroadcastOn("on");
        broadcast.setScheduleDate(new LocalDate(2010, 3, 20));
        
        DBObject dbObject = brt.toDBObject(null, broadcast);
        Broadcast b = brt.fromDBObject(dbObject, null);
        
        assertEquals(broadcast.getScheduleDate(), b.getScheduleDate());
        assertEquals(broadcast.getTransmissionTime().getMillis(), b.getTransmissionTime().getMillis());
        assertEquals(broadcast.getBroadcastOn(), b.getBroadcastOn());
        assertEquals(broadcast.getScheduleDate(), b.getScheduleDate());
    }
}
