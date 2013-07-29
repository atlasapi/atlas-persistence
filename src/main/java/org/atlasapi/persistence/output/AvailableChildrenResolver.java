package org.atlasapi.persistence.output;

import org.atlasapi.application.ApplicationConfiguration;
import org.atlasapi.media.entity.Container;

public interface AvailableChildrenResolver {

    Iterable<String> availableChildrenFor(Container container, ApplicationConfiguration config);

}