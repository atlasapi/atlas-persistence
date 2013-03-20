package org.atlasapi.persistence.media.entity;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.List;

import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.ModelTranslator;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class PersonTranslator implements ModelTranslator<Person> {
    private static final String GIVEN_NAME_KEY = "givenName";
    private static final String FAMILY_NAME_KEY = "familyName";
    private static final String GENDER_KEY = "gender";
    private static final String BIRTH_DATE_KEY = "birthDate";
    private static final String BIRTH_PLACE_KEY = "birthPlace";
    private static final String QUOTES_KEY = "quotes";
    
    private ContentGroupTranslator contentGroupTranslator = new ContentGroupTranslator(false);
    private ChildRefTranslator childRefTranslator = new ChildRefTranslator();
    
    @Override
    public Person fromDBObject(DBObject dbObject, Person entity) {
        if (entity == null) {
            entity = new Person();
        }
        
        entity.setGivenName(TranslatorUtils.toString(dbObject, GIVEN_NAME_KEY));
        entity.setFamilyName(TranslatorUtils.toString(dbObject, FAMILY_NAME_KEY));
        entity.setGender(TranslatorUtils.toString(dbObject, GENDER_KEY));
        entity.setBirthDate(TranslatorUtils.toDateTime(dbObject, BIRTH_DATE_KEY));
        entity.setBirthPlace(TranslatorUtils.toString(dbObject, BIRTH_PLACE_KEY));
        entity.setQuotes(TranslatorUtils.toSet(dbObject, QUOTES_KEY));
        
        contentGroupTranslator.fromDBObject(dbObject, entity);
        
        return entity;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, Person entity) {
        dbObject = contentGroupTranslator.toDBObject(dbObject, entity);
        
        TranslatorUtils.from(dbObject, GIVEN_NAME_KEY, entity.getGivenName());
        TranslatorUtils.from(dbObject, FAMILY_NAME_KEY, entity.getFamilyName());
        TranslatorUtils.from(dbObject, GENDER_KEY, entity.getGender());
        TranslatorUtils.fromDateTime(dbObject, BIRTH_DATE_KEY, entity.getBirthDate());
        TranslatorUtils.from(dbObject, BIRTH_PLACE_KEY, entity.getBirthPlace());
        TranslatorUtils.fromSet(dbObject, entity.getQuotes(), QUOTES_KEY);
        
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
