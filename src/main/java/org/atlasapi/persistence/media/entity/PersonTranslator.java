package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.ModelTranslator;

import com.mongodb.DBObject;

public class PersonTranslator implements ModelTranslator<Person> {
    
    private PlaylistTranslator playlistTranslator = new PlaylistTranslator();
    
    @Override
    public Person fromDBObject(DBObject dbObject, Person entity) {
        if (entity == null) {
            entity = new Person();
        }
        
        playlistTranslator.fromDBObject(dbObject, entity);
        
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Person entity) {
        dbObject = playlistTranslator.toDBObject(dbObject, entity);
        
        dbObject.put("type", Person.class.getSimpleName());
        
        return dbObject;
    }
}
