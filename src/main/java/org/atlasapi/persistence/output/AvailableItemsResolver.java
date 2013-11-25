package org.atlasapi.persistence.output;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.application.v3.ApplicationConfiguration;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Person;

public interface AvailableItemsResolver {

    Iterable<ChildRef> availableItemsFor(Container container, ApplicationConfiguration config);
    
    Iterable<ChildRef> availableItemsFor(Person person, ApplicationConfiguration config);

}