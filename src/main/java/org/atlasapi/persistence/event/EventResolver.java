package org.atlasapi.persistence.event;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Topic;

import com.google.common.base.Optional;


public interface EventResolver {

    Optional<Event> person(String uri);
    
    Optional<Event> fetch(Long id);
    
    Iterable<Event> fetchByEventGroup(Topic event);
}
