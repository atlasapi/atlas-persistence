package org.atlasapi.persistence.content.organisation;

import static org.atlasapi.persistence.events.EventTranslatorTest.createTopic;
import static org.junit.Assert.assertEquals;

import org.atlasapi.media.entity.Organisation;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.media.entity.OrganisationTranslator;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;


public class OrganisationTranslatorTest {

    private final OrganisationTranslator translator = new OrganisationTranslator();
    
    @Test
    public void testOrganisationTranslation() {
        Organisation organisation = createOrganisation();
        
        Organisation translated = translator.fromDBObject(translator.toDBObject(organisation));
        
        assertEquals(organisation.members(), translated.members());
        assertEquals(organisation.eventGroups(), translated.eventGroups());
    }
    
    public static Organisation createOrganisation() {
        Organisation organisation = new Organisation(ImmutableList.of(createPerson("dbpedia.org/person3", "person:3")));
        organisation.setCanonicalUri("dbpedia.org/Person3");
        organisation.setEventGroups(ImmutableSet.of(createTopic("example.org", "EVENT_GROUP")));
        return organisation;
    }

    public static Person createPerson(String uri, String curie) {
        return new Person(uri, curie, Publisher.METABROADCAST);
    }

}
