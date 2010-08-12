package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Broadcast;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class BroadcastTranslator  {
	
    private static final String TRANSMISSION_END_TIME_KEY = "transmissionEndTime";
	private static final String TRANSMISSION_TIME_KEY = "transmissionTime";
    
    public Broadcast fromDBObject(DBObject dbObject) {
        
        String broadcastOn = (String) dbObject.get("broadcastOn");
        DateTime transmissionTime = TranslatorUtils.toDateTime(dbObject, TRANSMISSION_TIME_KEY);
		
        Integer duration = (Integer) dbObject.get("broadcastDuration");
        
        Broadcast broadcast = new Broadcast(broadcastOn, transmissionTime, Duration.standardSeconds(duration));
        
        broadcast.setScheduleDate(TranslatorUtils.toLocalDate(dbObject, "scheduleDate"));
        broadcast.setAliases(TranslatorUtils.toSet(dbObject, DescriptionTranslator.ALIASES));
        broadcast.setLastUpdated(TranslatorUtils.toDateTime(dbObject, DescriptionTranslator.LAST_UPDATED));
        
        return broadcast;
    }

    public DBObject toDBObject(Broadcast entity) {
    	DBObject dbObject = new BasicDBObject();
        TranslatorUtils.from(dbObject, "broadcastDuration", entity.getBroadcastDuration());
        TranslatorUtils.from(dbObject, "broadcastOn", entity.getBroadcastOn());
        TranslatorUtils.fromLocalDate(dbObject, "scheduleDate", entity.getScheduleDate());
        TranslatorUtils.fromDateTime(dbObject, TRANSMISSION_TIME_KEY, entity.getTransmissionTime());
        TranslatorUtils.fromDateTime(dbObject, TRANSMISSION_END_TIME_KEY, entity.getTransmissionEndTime());
        TranslatorUtils.fromSet(dbObject, entity.getAliases(), DescriptionTranslator.ALIASES);
        TranslatorUtils.fromDateTime(dbObject, DescriptionTranslator.LAST_UPDATED, entity.getLastUpdated());
        return dbObject;
    }

}
