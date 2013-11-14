package org.atlasapi.persistence.output;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Person;

public interface UpcomingItemsResolver {

    Iterable<ChildRef> upcomingItemsFor(Container container);
    
    Iterable<ChildRef> upcomingItemsFor(Person person);

}