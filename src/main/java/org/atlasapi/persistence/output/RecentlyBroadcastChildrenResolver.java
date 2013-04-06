package org.atlasapi.persistence.output;

import org.atlasapi.media.content.Container;

public interface RecentlyBroadcastChildrenResolver {

    Iterable<String> recentlyBroadcastChildrenFor(Container container, int limit);

}
