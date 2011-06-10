package org.atlasapi.persistence.content.mongo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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
        
        List<String> items = Lists.newArrayList();
        for (int i=0; i<10; i++) {
            Item item = new Item("item"+i, "item"+i, Publisher.BBC);
            items.add(item.getCanonicalUri());
            person.addContents(item.childRef());
            store.updatePersonItems(person);
        }
        
        for (int i=0; i<10; i++) {
            Item item = new Item("item"+i, "item"+i, Publisher.BBC);
            person.addContents(item.childRef());
            store.updatePersonItems(person);
        }
        
        Person found = store.person(uri);
        assertNotNull(found);
        assertEquals(uri, found.getCanonicalUri());
        assertEquals(person.getPublisher(), found.getPublisher());
        
        assertEquals(10, found.getContents().size());
        assertEquals(items, ImmutableList.copyOf(Iterables.transform(found.getContents(), ChildRef.TO_URI)));
    }
}
