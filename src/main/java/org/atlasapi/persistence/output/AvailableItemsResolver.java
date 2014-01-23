package org.atlasapi.persistence.output;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.Multimap;

public interface AvailableItemsResolver {

    Iterable<ChildRef> availableItemsFor(Container container, ApplicationConfiguration config);
    
    Iterable<ChildRef> availableItemsFor(Person person, ApplicationConfiguration config);
    
    Multimap<Publisher, ChildRef> availableItemsByPublisherFor(Item item, ApplicationConfiguration config);

    Multimap<Publisher, ChildRef> availableItemsByPublisherFor(Container container, ApplicationConfiguration config);

}