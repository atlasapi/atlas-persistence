package org.atlasapi.persistence.content.mongo;

import java.util.List;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.persistence.content.PeopleResolver;
import org.atlasapi.persistence.content.PersonWriter;
import org.atlasapi.persistence.media.entity.PersonTranslator;
import org.joda.time.DateTime;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.DateTimeZones;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoPersonStore implements PersonWriter, PeopleResolver {
    
    private final DBCollection collection;
    private final PersonTranslator translator = new PersonTranslator();

    public MongoPersonStore(DatabasedMongo db) {
        collection = db.collection("people");
    }

    @Override
    public void addItemToPerson(Person person, Item item) {
        person.addContents(item);
        
        DBObject idQuery = translator.idQuery(person.getCanonicalUri()).build();
        
        collection.update(idQuery, translator.updateContentUris(person), true, false);
    }

    @Override
    public Person person(String uri) {
        List<Person> people = translator.fromDBObjects(translator.idQuery(uri).find(collection));
        return Iterables.getFirst(people, null);
    }

    @Override
    public void createOrUpdatePerson(Person person) {
        person.setLastUpdated(new DateTime(DateTimeZones.UTC));
        person.setMediaType(null);
        
        DBObject idQuery = translator.idQuery(person.getCanonicalUri()).build();
        collection.update(idQuery, translator.toDBObject(null, person), true, false);
    }
}
