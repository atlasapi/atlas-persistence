package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.CrewMember.Role;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;

public class PersonTranslator implements ModelTranslator<Person> {
    
    @Override
    public Person fromDBObject(DBObject dbObject, Person model) {
        String type = (String) dbObject.get("type");
        String uri = (String) dbObject.get("uri");
        String curie = (String) dbObject.get("curie");
        Publisher publisher = Publisher.fromKey((String) dbObject.get("publisher")).requireValue();
        
        Person person = null;
        if (type.equals(Actor.class.getSimpleName())) {
            person = new Actor(uri, curie, publisher).withCharacter((String) dbObject.get("character"));
        } else if (type.equals(CrewMember.class.getSimpleName())) {
            Role role = Role.fromKey((String) dbObject.get("role"));
            person = new CrewMember(uri, curie, publisher).withRole(role);
        }
        person.withProfileLink((String) dbObject.get("profileLink")).withName((String) dbObject.get("name"));
        
        return person;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Person model) {
        
        dbObject.put("type", model.getClass().getSimpleName());
        TranslatorUtils.from(dbObject, "name", model.name());
        TranslatorUtils.from(dbObject, "profileLink", model.profileLink());
        TranslatorUtils.from(dbObject, "uri", model.getCanonicalUri());
        TranslatorUtils.from(dbObject, "curie", model.getCurie());
        TranslatorUtils.from(dbObject, "publisher", model.getPublisher().key());
        
        if (model instanceof Actor) {
            Actor actor = (Actor) model;
            TranslatorUtils.from(dbObject, "character", actor.character());
        }
        
        if (model instanceof CrewMember) {
            CrewMember crew = (CrewMember) model;
            TranslatorUtils.from(dbObject, "role", crew.role().key());
        }
        
        return dbObject;
    }
}
