package org.atlasapi.persistence.content;

import java.util.concurrent.TimeUnit;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.people.PersonWriter;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.logging.SystemOutAdapterLog;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.integration.junit4.JMock;
import org.jmock.integration.junit4.JUnit4Mockery;
import org.jmock.integration.junit4.JUnitRuleMockery;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.metabroadcast.common.time.DateTimeZones;

public class QueuingPersonWriterTest {
    
    @Rule public final JUnitRuleMockery context = new JUnitRuleMockery();
    
    private final PersonWriter personWriter = context.mock(PersonWriter.class);
    
    private final Person person1 = new Person("person1", "person1", Publisher.BBC);
    private final Person person2 = new Person("person2", "person2", Publisher.BBC);
    
    private final Item item1 = new Item("item1", "item1", Publisher.BBC);
    private final Item item2 = new Item("item2", "item2", Publisher.BBC);
    
    private final DeterministicScheduler scheduler = new DeterministicScheduler();
    private QueuingPersonWriter writer;
    
    @Before
    public void setUp() {
        item1.setId(1);
        item1.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        item2.setId(2);
        item2.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        person1.setId(3);
        person2.setId(4);
        writer = new QueuingPersonWriter(personWriter, new SystemOutAdapterLog(), scheduler);
    }
    
    @Test
    public void shouldWritePersonOnlyOnce() {
        writer.addItemToPerson((Person) person1.copy(), item1);
        writer.addItemToPerson((Person) person2.copy(), item1);
        writer.addItemToPerson((Person) person1.copy(), item2);
        writer.addItemToPerson((Person) person2.copy(), item2);
        
        context.checking(new Expectations() {{
            oneOf(personWriter).createOrUpdatePerson(person1);
            oneOf(personWriter).createOrUpdatePerson(person2);
            
            allowing(personWriter).updatePersonItems(person1);
            allowing(personWriter).updatePersonItems(person2);
        }});
        
        scheduler.tick(15, TimeUnit.MINUTES);
    }
}
