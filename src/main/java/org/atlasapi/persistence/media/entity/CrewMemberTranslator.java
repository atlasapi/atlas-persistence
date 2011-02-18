package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Actor;
import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.CrewMember.Role;

import com.metabroadcast.common.persistence.translator.ModelTranslator;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class CrewMemberTranslator implements ModelTranslator<CrewMember> {
    
    @Override
    public CrewMember fromDBObject(DBObject dbObject, CrewMember model) {
        Role role = dbObject.containsField("role") ? Role.fromKey((String) dbObject.get("role")) : Role.ACTOR;
        String uri = (String) dbObject.get("uri");
        String curie = (String) dbObject.get("curie");
        Publisher publisher = Publisher.fromKey((String) dbObject.get("publisher")).requireValue();
        
        CrewMember crew = null;
        if (Role.ACTOR == role) {
            crew = new Actor(uri, curie, publisher).withCharacter((String) dbObject.get("character"));
        } else {
            crew = new CrewMember(uri, curie, publisher).withRole(role);
        }
        crew.withProfileLinks(TranslatorUtils.toSet(dbObject, "profileLinks")).withName((String) dbObject.get("name"));
        
        return crew;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, CrewMember model) {
        dbObject = new BasicDBObject();
        
        TranslatorUtils.from(dbObject, "name", model.name());
        TranslatorUtils.fromSet(dbObject, model.profileLinks(), "profileLinks");
        TranslatorUtils.from(dbObject, "uri", model.getCanonicalUri());
        TranslatorUtils.from(dbObject, "curie", model.getCurie());
        TranslatorUtils.from(dbObject, "publisher", model.publisher().key());
        TranslatorUtils.from(dbObject, "role", model.role().key());
        
        if (model instanceof Actor) {
            Actor actor = (Actor) model;
            TranslatorUtils.from(dbObject, "character", actor.character());
        }
        
        return dbObject;
    }

}
