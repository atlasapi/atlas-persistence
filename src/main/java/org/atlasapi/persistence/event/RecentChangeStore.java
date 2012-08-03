package org.atlasapi.persistence.event;

import org.atlasapi.messaging.EntityUpdatedMessage;

public interface RecentChangeStore {

    /**
     * Log a change.
     * @param event
     */
    void logChange(EntityUpdatedMessage event);
    
    /**
     * Logged changes
     * @return
     */
    Iterable<EntityUpdatedMessage> changes();
    
}
