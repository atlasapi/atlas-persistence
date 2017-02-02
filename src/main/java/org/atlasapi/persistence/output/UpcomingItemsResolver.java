package org.atlasapi.persistence.output;

import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.Multimap;

public interface UpcomingItemsResolver {

    Iterable<ChildRef> upcomingItemsFor(Container container);
    
    Iterable<ChildRef> upcomingItemsFor(Person person);
    
    Multimap<Publisher, ChildRef> upcomingItemsByPublisherFor(Container container);
    
    Multimap<Publisher, ChildRef> upcomingItemsByPublisherFor(Item container, ApplicationConfiguration config);

}