package org.atlasapi.persistence.output;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Person;

public interface UpcomingItemsResolver {

    Iterable<String> upcomingItemsFor(Container container);
    
    Iterable<String> upcomingItemsFor(Person person);

}