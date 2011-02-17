package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.CrewMember.Role;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;

public class PersonTranslator implements ModelTranslator<Person> {
    
    private final DescriptionTranslator descriptionTranslator = new DescriptionTranslator();

    @Override
    public Person fromDBObject(DBObject dbObject, Person model) {
        String type = (String) dbObject.get("type");
        
        Person person = null;
        if (type.equals(Actor.class.getSimpleName())) {
            person = new Actor().withCharacter((String) dbObject.get("character"));
        } else if (type.equals(CrewMember.class.getSimpleName())) {
            Role role = Role.fromKey((String) dbObject.get("role"));
            person = new CrewMember().withRole(role);
        }
        person.withProfileLink((String) dbObject.get("profileLink")).withName((String) dbObject.get("name"));
        descriptionTranslator.fromDBObject(dbObject, person);
        
        return person;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Person model) {
        dbObject = descriptionTranslator.toDBObject(dbObject, model);
        
        dbObject.put("type", model.getClass().getSimpleName());
        TranslatorUtils.from(dbObject, "name", model.name());
        TranslatorUtils.from(dbObject, "profileLink", model.profileLink());
        
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
