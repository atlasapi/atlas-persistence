package org.atlasapi.persistence.output;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Person;

public interface AvailableItemsResolver {

    Iterable<String> availableItemsFor(Container container, ApplicationConfiguration config);
    
    Iterable<String> availableItemsFor(Person person, ApplicationConfiguration config);

}