package org.atlasapi.persistence.event;

import org.atlasapi.media.entity.Event;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class EventTranslator {

    private final IdentifiedTranslator identifiedTranslator = new IdentifiedTranslator(true);
    
    public DBObject toDBObject(Event event) {
        DBObject dbo = new BasicDBObject();
        
        identifiedTranslator.fromDBObject(dbo, event);
        //TODO 
        
        return dbo;
    }

    public Event fromDBObject(DBObject dbObject) {
        if (dbObject == null) {
            return null;
        }
        
        Event event = Event.builder()
                // TODO
                .build();
        
        return (Event) identifiedTranslator.fromDBObject(dbObject, event);
    }
}
