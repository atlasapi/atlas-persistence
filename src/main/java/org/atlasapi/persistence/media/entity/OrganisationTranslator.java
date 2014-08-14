package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.Organisation;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class OrganisationTranslator {

    public static final String MEMBERS_KEY = "members";
    
    private final ContentGroupTranslator contentGroupTranslator = new ContentGroupTranslator(false);
    private final PersonTranslator personTranslator = new PersonTranslator();
    private final ChildRefTranslator childRefTranslator = new ChildRefTranslator();
    
    public DBObject toDBObject(Organisation model) {
        DBObject dbo = new BasicDBObject();
        
        contentGroupTranslator.toDBObject(dbo, model);
        TranslatorUtils.fromIterable(dbo, MEMBERS_KEY, model.members(), personTranslator.translatePerson());
        
        return dbo;
    }
    
    public Organisation fromDBObject(DBObject dbo) {
        if (dbo == null) {
            return null;
        }
        Organisation organisation = new Organisation(personTranslator.fromDBObjects(TranslatorUtils.toDBObjectList(dbo, MEMBERS_KEY)));
        contentGroupTranslator.fromDBObject(dbo, organisation);
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
}
