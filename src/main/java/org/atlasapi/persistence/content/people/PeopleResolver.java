package org.atlasapi.persistence.content.people;

import org.atlasapi.media.entity.Person;

public interface PeopleResolver {

    Person person(String uri);
    
}
