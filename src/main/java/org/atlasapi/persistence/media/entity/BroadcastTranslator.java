package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Broadcast;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class BroadcastTranslator  {
	
    private static final String NEW_SERIES_KEY = "newSeries";
    private static final String PREMIER_KEY = "premier";
    private static final String TRANSMISSION_END_TIME_KEY = "transmissionEndTime";
	private static final String TRANSMISSION_TIME_KEY = "transmissionTime";
	private static final String REPEAT_KEY = "repeat";
	private static final String SUBTITLED_KEY = "subtitled";
	private static final String SIGNED_KEY = "signed";
	private static final String AUDIO_DESCRIBED_KEY = "audioDescribed";
	private static final String HD_KEY = "highDefinition";
	private static final String WIDESCREEN_KEY = "widescreen";
	private static final String SURROUND_KEY = "surround";
	private static final String LIVE_KEY = "live";
	
    
    public Broadcast fromDBObject(DBObject dbObject) {
        
        String broadcastOn = (String) dbObject.get("broadcastOn");
        DateTime transmissionTime = TranslatorUtils.toDateTime(dbObject, TRANSMISSION_TIME_KEY);
		
        Integer duration = (Integer) dbObject.get("broadcastDuration");
        Boolean activelyPublished = (dbObject.containsField("activelyPublished") ? (Boolean) dbObject.get("activelyPublished") : Boolean.TRUE);
        String id = (String) dbObject.get("id");
        
        Broadcast broadcast = new Broadcast(broadcastOn, transmissionTime, Duration.standardSeconds(duration), activelyPublished).withId(id);
        
        broadcast.setScheduleDate(TranslatorUtils.toLocalDate(dbObject, "scheduleDate"));
        broadcast.setAliases(TranslatorUtils.toSet(dbObject, DescriptionTranslator.ALIASES));
        broadcast.setLastUpdated(TranslatorUtils.toDateTime(dbObject, DescriptionTranslator.LAST_UPDATED));
        broadcast.setRepeat(TranslatorUtils.toBoolean(dbObject, REPEAT_KEY));
        broadcast.setSubtitled(TranslatorUtils.toBoolean(dbObject, SUBTITLED_KEY));
        broadcast.setSigned(TranslatorUtils.toBoolean(dbObject, SIGNED_KEY));
        broadcast.setAudioDescribed(TranslatorUtils.toBoolean(dbObject, AUDIO_DESCRIBED_KEY));
        broadcast.setHighDefinition(TranslatorUtils.toBoolean(dbObject, HD_KEY));
        broadcast.setWidescreen(TranslatorUtils.toBoolean(dbObject, WIDESCREEN_KEY));
        broadcast.setSurround(TranslatorUtils.toBoolean(dbObject, SURROUND_KEY));
        broadcast.setLive(TranslatorUtils.toBoolean(dbObject, LIVE_KEY));
        broadcast.setPremiere(TranslatorUtils.toBoolean(dbObject, PREMIER_KEY));
        broadcast.setNewSeries(TranslatorUtils.toBoolean(dbObject, NEW_SERIES_KEY));
        
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
        TranslatorUtils.from(dbObject, "activelyPublished", entity.isActivelyPublished());
        TranslatorUtils.from(dbObject, "id", entity.getId());
        TranslatorUtils.from(dbObject, REPEAT_KEY, entity.isRepeat());
        TranslatorUtils.from(dbObject, SUBTITLED_KEY, entity.isSubtitled());
        TranslatorUtils.from(dbObject, SIGNED_KEY, entity.isSigned());
        TranslatorUtils.from(dbObject, AUDIO_DESCRIBED_KEY, entity.isAudioDescribed());
        TranslatorUtils.from(dbObject, HD_KEY, entity.isHighDefinition());
        TranslatorUtils.from(dbObject, WIDESCREEN_KEY, entity.isWidescreen());
        TranslatorUtils.from(dbObject, SURROUND_KEY, entity.isSurround());
        TranslatorUtils.from(dbObject, LIVE_KEY, entity.isLive());
        TranslatorUtils.from(dbObject, PREMIER_KEY, entity.isPremiere());
        TranslatorUtils.from(dbObject, NEW_SERIES_KEY, entity.isNewSeries());
        return dbObject;
    }

}
