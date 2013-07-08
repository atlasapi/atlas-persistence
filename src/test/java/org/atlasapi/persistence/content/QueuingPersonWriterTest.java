package org.atlasapi.persistence.content;

import java.util.concurrent.TimeUnit;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.people.PersonStore;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.logging.SystemOutAdapterLog;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.base.Optional;

@RunWith(JMock.class)
public class QueuingPersonWriterTest {
    private Mockery context = new JUnit4Mockery();
    
    private final PersonStore personStore = context.mock(PersonStore.class);
    
    private final Person person1 = new Person("person1", "person1", Publisher.BBC);
    private final Person person2 = new Person("person2", "person2", Publisher.BBC);
    
    private final Item item1 = new Item("item1", "item1", Publisher.BBC);
    private final Item item2 = new Item("item2", "item2", Publisher.BBC);
    
    private final DeterministicScheduler scheduler = new DeterministicScheduler();
    private QueuingPersonWriter writer;
    
    @Before
    public void setUp() {
        writer = new QueuingPersonWriter(personStore, new SystemOutAdapterLog(), scheduler);
    }
    
    @Test
    public void shouldWritePersonOnlyOnce() {
        writer.addItemToPerson((Person) person1.copy(), item1);
        writer.addItemToPerson((Person) person2.copy(), item1);
        writer.addItemToPerson((Person) person1.copy(), item2);
        writer.addItemToPerson((Person) person2.copy(), item2);
        
        context.checking(new Expectations() {{
            oneOf(personStore).person(person1.getCanonicalUri()); will(returnValue(Optional.absent()));
            oneOf(personStore).person(person2.getCanonicalUri()); will(returnValue(person2));
            oneOf(personStore).createOrUpdatePerson(person1);
            
            allowing(personStore).updatePersonItems(person1);
            allowing(personStore).updatePersonItems(person2);
        }});
        
        scheduler.tick(15, TimeUnit.MINUTES);
    }
}
