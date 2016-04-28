package org.atlasapi.persistence.event;

import org.atlasapi.media.entity.Event;


public interface EventWriter {

    Event createOrUpdate(Event event);
}
