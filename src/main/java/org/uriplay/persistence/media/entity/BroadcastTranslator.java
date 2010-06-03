package org.uriplay.persistence.media.entity;

import org.uriplay.media.entity.Broadcast;

import com.metabroadcast.common.persistence.translator.ModelTranslator;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;

public class BroadcastTranslator implements ModelTranslator<Broadcast> {
    private DescriptionTranslator descriptionTranslator;
    
    public BroadcastTranslator(DescriptionTranslator descriptionTranslator) {
        this.descriptionTranslator = descriptionTranslator;
    }

    @Override
    public Broadcast fromDBObject(DBObject dbObject, Broadcast entity) {
        if (entity == null) {
            entity = new Broadcast();
        }
        
        descriptionTranslator.fromDBObject(dbObject, entity);
        entity.setBroadcastDuration((Integer) dbObject.get("broadcastDuration"));
        entity.setBroadcastOn((String) dbObject.get("broadcastOn"));
        entity.setScheduleDate(TranslatorUtils.toLocalDate(dbObject, "scheduleDate"));
        entity.setTransmissionTime(TranslatorUtils.toDateTime(dbObject, "transmissionTime"));
        
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Broadcast entity) {
        dbObject = descriptionTranslator.toDBObject(dbObject, entity);
        
        TranslatorUtils.from(dbObject, "broadcastDuration", entity.getBroadcastDuration());
        TranslatorUtils.from(dbObject, "broadcastOn", entity.getBroadcastOn());
        TranslatorUtils.fromLocalDate(dbObject, "scheduleDate", entity.getScheduleDate());
        TranslatorUtils.fromDateTime(dbObject, "transmissionTime", entity.getTransmissionTime());
        
        return dbObject;
    }

}
