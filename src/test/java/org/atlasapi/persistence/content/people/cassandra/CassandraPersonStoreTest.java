package org.atlasapi.persistence.content.people.cassandra;

import java.util.Arrays;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.cassandra.BaseCassandraTest;
import org.junit.Test;
import org.junit.Before;
import org.joda.time.DateTime;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 */
@Ignore(value = "Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraPersonStoreTest extends BaseCassandraTest {

    private CassandraPersonStore store;

    @Before
    @Override
    public void before() {
        super.before();
        store = new CassandraPersonStore(context, 10000);
    }

    @Test
    public void testPerson() {
        Person person = new Person("person1", "curie", Publisher.METABROADCAST);
        person.addContent(new ChildRef(1L, "child1", "", new DateTime(), EntityType.ITEM));

        store.createOrUpdatePerson(person);

        assertEquals(person, store.person("person1"));
        assertEquals("child1", store.person("person1").getContents().get(0).getUri());
    }

    @Test
    public void testUpdatePersonContents() {
        Person person = new Person("person1", "curie", Publisher.METABROADCAST);
        person.addContent(new ChildRef(1L, "child1", "", new DateTime(), EntityType.ITEM));

        store.createOrUpdatePerson(person);
        
        person.setContents(Arrays.asList(new ChildRef(2L, "child2", "", new DateTime(), EntityType.ITEM)));

        store.createOrUpdatePerson(person);
        
        assertEquals(person, store.person("person1"));
        assertEquals("child2", store.person("person1").getContents().get(0).getUri());
    }
}
