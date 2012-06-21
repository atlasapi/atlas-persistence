package org.atlasapi.persistence.event;

import org.atlasapi.messaging.event.EntityUpdatedEvent;

public interface RecentChangeStore {

    /**
     * Log a change.
     * @param event
     */
    void logChange(EntityUpdatedEvent event);
    
    /**
     * Logged changes
     * @return
     */
    Iterable<EntityUpdatedEvent> changes();
    
}
