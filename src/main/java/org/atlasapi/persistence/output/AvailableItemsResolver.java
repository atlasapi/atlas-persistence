package org.atlasapi.persistence.output;

import com.metabroadcast.applications.client.model.internal.Application;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.Multimap;

public interface AvailableItemsResolver {

    Iterable<ChildRef> availableItemsFor(Container container, Application application);
    
    Iterable<ChildRef> availableItemsFor(Person person, Application application);
    
    Multimap<Publisher, ChildRef> availableItemsByPublisherFor(Item item, Application application);

    Multimap<Publisher, ChildRef> availableItemsByPublisherFor(Container container, Application application);

}