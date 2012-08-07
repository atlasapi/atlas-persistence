package org.atlasapi.persistence.media.entity;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.List;

import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.media.ModelTranslator;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class PersonTranslator implements ModelTranslator<Person> {
    
    private ContentGroupTranslator contentGroupTranslator = new ContentGroupTranslator(false);
    private ChildRefTranslator childRefTranslator = new ChildRefTranslator();
    
    @Override
    public Person fromDBObject(DBObject dbObject, Person entity) {
        if (entity == null) {
            entity = new Person();
        }
        
        contentGroupTranslator.fromDBObject(dbObject, entity);
        
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Person entity) {
        dbObject = contentGroupTranslator.toDBObject(dbObject, entity);
        
        dbObject.put("type", Person.class.getSimpleName());
        dbObject.removeField(ContentGroupTranslator.CONTENT_URIS_KEY);
        
        return dbObject;
    }
    
    public DBObject updateContentUris(Person entity) {
        return new BasicDBObject(MongoConstants.ADD_TO_SET, new BasicDBObject(ContentGroupTranslator.CONTENT_URIS_KEY, new BasicDBObject("$each", childRefTranslator.toDBList(entity.getContents()))));
    }
    
    public List<Person> fromDBObjects(Iterable<DBObject> dbObjects) {
        ImmutableList.Builder<Person> people = ImmutableList.builder();
        for (DBObject dbObject: dbObjects) {
            people.add(fromDBObject(dbObject, null));
        }
        return people.build();
    }
    
    public MongoQueryBuilder idQuery(String uri) {
        return where().idEquals(uri);
    }
}
