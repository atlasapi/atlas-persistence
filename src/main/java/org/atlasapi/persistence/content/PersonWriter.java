package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Person;

public interface PersonWriter {

    void updatePersonItems(Person person);
    
    void createOrUpdatePerson(Person person);
}
