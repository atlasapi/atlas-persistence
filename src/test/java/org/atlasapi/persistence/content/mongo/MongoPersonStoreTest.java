package org.atlasapi.persistence.content.mongo;

import static org.junit.Assert.*;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.junit.Before;
import org.junit.Test;

import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;


public class MongoPersonStoreTest {

    private DatabasedMongo db;
    private MongoPersonStore store;
    
    private final String uri = "person1";
    
    @Before
    public void setUp() {
        db = MongoTestHelper.anEmptyTestDatabase();
        store = new MongoPersonStore(db);
    }
    
    @Test
    public void shouldSetPersonAndAddItems() {
        Person person = new Person(uri, uri, Publisher.BBC);
        store.createOrUpdatePerson(person);
        
        for (int i=0; i<10; i++) {
            Item item = new Item("item"+i, "item"+i, Publisher.BBC);
            store.addItemToPerson(person, item);
        }
        
        Person found = store.person(uri);
        assertNotNull(found);
        assertEquals(uri, found.getCanonicalUri());
        assertEquals(person.getPublisher(), found.getPublisher());
        
        assertEquals(10, found.getContentUris().size());
        assertEquals(person.getContentUris(), found.getContentUris());
    }
}
