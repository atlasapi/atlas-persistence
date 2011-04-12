package org.atlasapi.persistence.media.entity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.atlasapi.media.entity.Broadcast;
import org.joda.time.Duration;
import org.joda.time.LocalDate;
import org.junit.Test;

import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class BroadcastTranslatorTest {
    BroadcastTranslator brt = new BroadcastTranslator();
    SystemClock clock = new SystemClock();
    private DBCollection collection = MongoTestHelper.anEmptyTestDatabase().collection("broadcasts");
    
    @Test
    public void testFromBroadcast() {
		Broadcast broadcast = new Broadcast("on", clock.now(), Duration.standardSeconds(1));
        
        DBObject dbObject = brt.toDBObject(broadcast);
        assertEquals(broadcast.getBroadcastDuration(), dbObject.get("broadcastDuration"));
    }
    
    @Test
    public void testToBroadcast() {
		Broadcast broadcast = new Broadcast("on", clock.now(), Duration.standardSeconds(1));
        broadcast.setScheduleDate(new LocalDate(2010, 3, 20));
        broadcast.addAlias("some alias");
        broadcast.setRepeat(Boolean.TRUE);
        
        DBObject toSave = brt.toDBObject(broadcast);
        collection.save(toSave);
        
        DBObject loaded = collection.findOne();
        Broadcast b = brt.fromDBObject(loaded);
        
        assertEquals(broadcast.getScheduleDate(), b.getScheduleDate());
        assertEquals(broadcast.getTransmissionTime().getMillis(), b.getTransmissionTime().getMillis());
        assertEquals(broadcast.getBroadcastOn(), b.getBroadcastOn());
        assertEquals(broadcast.getScheduleDate(), b.getScheduleDate());
        assertEquals(broadcast.isRepeat(), b.isRepeat());
        assertFalse(b.getAliases().isEmpty());
    }
}
