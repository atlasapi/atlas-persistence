package org.atlasapi.persistence.output;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.Container;

public interface RecentlyBroadcastChildrenResolver {

    Iterable<Id> recentlyBroadcastChildrenFor(Container container, int limit);

}
