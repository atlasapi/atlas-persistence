package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;

public interface PersonWriter {

    void addItemToPerson(Person person, Item item);
    
    void createOrUpdatePerson(Person person);
    
}
