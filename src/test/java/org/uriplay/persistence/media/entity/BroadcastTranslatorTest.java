package org.uriplay.persistence.media.entity;

import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.uriplay.media.entity.Broadcast;

import com.mongodb.DBObject;

import junit.framework.TestCase;

public class BroadcastTranslatorTest extends TestCase {
    DescriptionTranslator dt = new DescriptionTranslator();
    BroadcastTranslator brt = new BroadcastTranslator(dt);
    
    public void testFromBroadcast() {
        Broadcast broadcast = new Broadcast();
        broadcast.setBroadcastDuration(1);
        broadcast.setTransmissionTime(new DateTime());
        
        DBObject dbObject = brt.toDBObject(null, broadcast);
        assertEquals(broadcast.getBroadcastDuration(), dbObject.get("broadcastDuration"));
        assertEquals(broadcast.getTransmissionTime().getMillis(), dbObject.get("transmissionTime"));
    }
    
    public void testToBroadcast() {
        Broadcast broadcast = new Broadcast();
        broadcast.setBroadcastDuration(1);
        broadcast.setTransmissionTime(new DateTime());
        broadcast.setBroadcastOn("on");
        broadcast.setScheduleDate(new LocalDate(2010, 3, 20));
        
        DBObject dbObject = brt.toDBObject(null, broadcast);
        Broadcast b = brt.fromDBObject(dbObject, null);
        
        assertEquals(broadcast.getScheduleDate(), b.getScheduleDate());
        assertEquals(broadcast.getTransmissionTime(), b.getTransmissionTime());
        assertEquals(broadcast.getBroadcastOn(), b.getBroadcastOn());
        assertEquals(broadcast.getScheduleDate(), b.getScheduleDate());
    }
}
