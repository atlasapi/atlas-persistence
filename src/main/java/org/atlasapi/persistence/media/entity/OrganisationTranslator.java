package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Topic;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class OrganisationTranslator {

    public static final String EVENT_GROUPS_KEY = "eventGroups";
    public static final String MEMBERS_KEY = "members";
    
    private final ContentGroupTranslator contentGroupTranslator = new ContentGroupTranslator(false);
    private final PersonTranslator personTranslator = new PersonTranslator();
    private final ChildRefTranslator childRefTranslator = new ChildRefTranslator();
    private final TopicTranslator topicTranslator = new TopicTranslator();
    
    public DBObject toDBObject(Organisation model) {
        DBObject dbo = new BasicDBObject();
        
        contentGroupTranslator.toDBObject(dbo, model);
        TranslatorUtils.fromIterable(dbo, MEMBERS_KEY, model.members(), personTranslator.translatePerson());
        TranslatorUtils.fromIterable(dbo, EVENT_GROUPS_KEY, model.eventGroups(), topicToDBObject());
        
        return dbo;
    }
    
    public Organisation fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        Optional<Iterable<Topic>> eventGroups = TranslatorUtils.toIterable(dbo, EVENT_GROUPS_KEY, topicFromDBObject());
        
        Organisation organisation = new Organisation(personTranslator.fromDBObjects(TranslatorUtils.toDBObjectList(dbo, MEMBERS_KEY)));
        contentGroupTranslator.fromDBObject(dbo, organisation);
        if (eventGroups.isPresent()) {
            organisation.setEventGroups(eventGroups.get());
        }
        
        return organisation;
    }
    
    public DBObject updateContentUris(Organisation entity) {
        return new BasicDBObject(MongoConstants.ADD_TO_SET, new BasicDBObject(ContentGroupTranslator.CONTENT_URIS_KEY, new BasicDBObject("$each", childRefTranslator.toDBList(entity.getContents()))));
    }
    
    public List<Organisation> fromDBObjects(Iterable<DBObject> dbObjects) {
        ImmutableList.Builder<Organisation> organisations = ImmutableList.builder();
        for (DBObject dbObject: dbObjects) {
            organisations.add(fromDBObject(dbObject));
        }
        return organisations.build();
    }
    
    public Function<Organisation, DBObject> translateToDBObject() {
        return new Function<Organisation, DBObject>() {
            @Override
            public DBObject apply(Organisation input) {
                return toDBObject(input);
            }
        };
    }
    
    public Function<DBObject, Organisation> translateFromDBObject() {
        return new Function<DBObject, Organisation>() {
            @Override
            public Organisation apply(DBObject input) {
                return fromDBObject(input);
            }
        };
    }
    
    private Function<DBObject, Topic> topicFromDBObject() {
        return new Function<DBObject, Topic>() {
            @Override
            public Topic apply(DBObject input) {
                return topicTranslator.fromDBObject(input);
            }
        };
    }
    
    private Function<Topic, DBObject> topicToDBObject() {
        return new Function<Topic, DBObject>() {
            @Override
            public DBObject apply(Topic input) {
                return topicTranslator.toDBObject(input);
            }
        };
    }
}
