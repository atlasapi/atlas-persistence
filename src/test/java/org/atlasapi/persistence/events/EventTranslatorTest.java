package org.atlasapi.persistence.events;

import static org.atlasapi.persistence.content.organisation.OrganisationTranslatorTest.createOrganisation;
import static org.atlasapi.persistence.content.organisation.OrganisationTranslatorTest.createPerson;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.event.EventTranslator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import com.google.common.collect.ImmutableList;


public class EventTranslatorTest {
    
    private final EventTranslator translator = new EventTranslator();

    @Test
    public void testTranslationToAndFromDBObject() {
        Event event = createEvent(createEventGroups());
        
        Event translated = translator.fromDBObject(translator.toDBObject(event));
        
        assertEquals(event.title(), translated.title());
        assertEquals(event.publisher(), translated.publisher());
        assertEquals(event.venue(), translated.venue());
        assertEquals(event.startTime(), translated.startTime());
        assertEquals(event.endTime(), translated.endTime());
        assertEquals(event.eventGroups(), translated.eventGroups());
        assertEquals(event.participants(), translated.participants());
        assertEquals(event.organisations(), translated.organisations());
    }

    public static Event createEvent(Iterable<Topic> eventGroups) {
        DateTime now = DateTime.now(DateTimeZone.UTC);
        return Event.builder()
                .withTitle("Title")
                .withPublisher(Publisher.METABROADCAST)
                .withVenue(createTopic("dbpedia.org/Allianz_Stadium", "Allianz Stadium"))
                .withStartTime(now.minusDays(2))
                .withEndTime(now)
                .withEventGroups(eventGroups)
                .withParticipants(createParticipants())
                .withOrganisations(createOrganisations())
                .build();
    }

    private static List<Organisation> createOrganisations() {
        return ImmutableList.of(createOrganisation());
    }

    private static List<Person> createParticipants() {
        return ImmutableList.of(
                createPerson("dbpedia.org/person1", "person:1"), 
                createPerson("dbpedia.org/person2", "person:2")
        );
    }

    private static List<Topic> createEventGroups() {
        return ImmutableList.of(
                createTopic("dbpedia.org/Football", "Football"), 
                createTopic("dbpedia.org/Premier_League", "Premier League")
        );
    }

    public static Topic createTopic(String uri, String value) {
        Topic topic = new Topic(1234l, "dbpedia", value);
        topic.setCanonicalUri(uri);
        topic.setPublisher(Publisher.METABROADCAST);
        return topic;
    }

}
