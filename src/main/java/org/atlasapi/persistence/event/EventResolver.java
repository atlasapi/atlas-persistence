package org.atlasapi.persistence.event;

import org.atlasapi.media.entity.Event;
import org.atlasapi.media.entity.Topic;

import com.google.common.base.Optional;


public interface EventResolver {

    Optional<Event> fetch(Long id);
    
    Optional<Event> fetch(String uri);
    
    Iterable<Event> fetchByEventGroup(Topic eventGroup);
    
    Iterable<Event> fetchAll();
}
