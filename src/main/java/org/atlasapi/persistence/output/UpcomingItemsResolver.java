package org.atlasapi.persistence.output;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;

public interface UpcomingItemsResolver {

    Iterable<ChildRef> upcomingItemsFor(Container container);
    
    Iterable<ChildRef> upcomingItemsFor(Person person);

    boolean hasUpcomingBroadcasts(Item item, ApplicationConfiguration config);

}