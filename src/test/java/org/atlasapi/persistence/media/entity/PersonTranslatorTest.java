package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Person;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class PersonTranslatorTest {

    @Test
    public void testPersonUpdate() {
        Person person = new Person();

        person.setFamilyName("familyName");
        person.setGivenName("givenName");
        person.setGender("gender");

        Person newPerson = new Person();
        newPerson.setGivenName("bill");
        newPerson.setFamilyName("Smith");

        PersonTranslator personTranslator = new PersonTranslator();

        DBObject dbObject = personTranslator.toDBObject(new BasicDBObject(), person);
        personTranslator.mainPersonFieldsToDBObject(dbObject, newPerson);
        Person resultPerson = personTranslator.fromDBObject(dbObject, new Person());

        assertEquals(newPerson.getFamilyName(), resultPerson.getFamilyName());
        assertEquals(newPerson.getGivenName(), resultPerson.getGivenName());
        assertEquals(person.getGender(), resultPerson.getGender());

    }
}
