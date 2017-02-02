package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Person;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.joda.time.DateTime;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class PersonTranslatorTest {

    private static final String GIVEN_NAME_KEY = "givenName";
    private static final String FAMILY_NAME_KEY = "familyName";
    private static final String GENDER_KEY = "gender";
    private static final String BIRTH_DATE_KEY = "birthDate";
    private static final String BIRTH_PLACE_KEY = "birthPlace";

    private static final String PSEUDO_FORENAME_KEY = "pseudoForname";
    private static final String PSEUDO_SURNAME_KEY = "pseudoSurname";
    private static final String ADDITIONAL_INFORMATION_KEY = "additionalInformation";
    private static final String BILLING_KEY = "billing";
    private static final String SOURCE_KEY = "source";
    private static final String SOURCE_TITLE_KEY = "sourceTitle";
    private PersonTranslator personTranslator;

    public PersonTranslatorTest() {
        personTranslator = new PersonTranslator();
    }

    @Test
    public void translationOfPersonToDatabaseObject() {
        Person person = makePerson();
        DBObject dbo = new BasicDBObject();
        personTranslator.toDBObject(dbo, person);

        assertTrue(dbo.containsField(GIVEN_NAME_KEY));
        assertTrue(dbo.containsField(FAMILY_NAME_KEY));
        assertTrue(dbo.containsField(GENDER_KEY));
        assertTrue(dbo.containsField(BIRTH_DATE_KEY));
        assertTrue(dbo.containsField(BIRTH_PLACE_KEY));
        assertTrue(dbo.containsField(PSEUDO_FORENAME_KEY));
        assertTrue(dbo.containsField(PSEUDO_SURNAME_KEY));
        assertTrue(dbo.containsField(ADDITIONAL_INFORMATION_KEY));
        assertTrue(dbo.containsField(BILLING_KEY));
        assertTrue(dbo.containsField(SOURCE_KEY));
        assertTrue(dbo.containsField(SOURCE_TITLE_KEY));

        assertEquals(person.getGivenName(), dbo.get(GIVEN_NAME_KEY));
        assertEquals(person.getFamilyName(), dbo.get(FAMILY_NAME_KEY));
        assertEquals(person.getGender(), dbo.get(GENDER_KEY));
        assertEquals(
                person.getBirthDate().compareTo(new DateTime(dbo.get(BIRTH_DATE_KEY))),
                0
        );
        assertEquals(person.getBirthPlace(), dbo.get(BIRTH_PLACE_KEY));
        assertEquals(person.getPseudoForename(), dbo.get(PSEUDO_FORENAME_KEY));
        assertEquals(person.getPseudoSurname(), dbo.get(PSEUDO_SURNAME_KEY));
        assertEquals(person.getAdditionalInfo(), dbo.get(ADDITIONAL_INFORMATION_KEY));
        assertEquals(person.getBilling(), dbo.get(BILLING_KEY));
        assertEquals(person.getSource(), dbo.get(SOURCE_KEY));
        assertEquals(person.getSourceTitle(), dbo.get(SOURCE_TITLE_KEY));
    }

    @Test
    public void translationOfPersonFromDatabaseObject() {
        Person person = makePerson();
        DBObject dbo = new BasicDBObject();
        personTranslator.toDBObject(dbo, person);

        Person output = personTranslator.fromDBObject(dbo, null);

        assertEquals(person.getGivenName(), output.getGivenName());
        assertEquals(person.getFamilyName(), output.getFamilyName());
        assertEquals(person.getGender(), output.getGender());
        assertEquals(person.getBirthDate().compareTo(output.getBirthDate()), 0);
        assertEquals(person.getBirthPlace(), output.getBirthPlace());
        assertEquals(person.getPseudoForename(), output.getPseudoForename());
        assertEquals(person.getPseudoSurname(), output.getPseudoSurname());
        assertEquals(person.getAdditionalInfo(), output.getAdditionalInfo());
        assertEquals(person.getBilling(), output.getBilling());
        assertEquals(person.getSource(), output.getSource());
        assertEquals(person.getSourceTitle(), output.getSourceTitle());
    }

    private Person makePerson() {
        Person person = new Person();
        person.setGivenName("bill");
        person.setFamilyName("bobby");
        person.setGender("male");
        person.setBirthDate(DateTime.now());
        person.setBirthPlace("Margate");

        person.setPseudoForename("ben");
        person.setPseudoSurname("wilkinson");
        person.setAdditionalInfo("none");
        person.setBilling("billing");
        person.setSource("source");
        person.setSourceTitle("sourceTitle");

        return person;
    }
}