package org.atlasapi.persistence.output;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.Container;

public interface UpcomingChildrenResolver {

    Iterable<Id> availableChildrenFor(Container container);

}