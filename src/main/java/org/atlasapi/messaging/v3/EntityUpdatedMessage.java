package org.atlasapi.messaging.v3;

import org.atlasapi.messaging.worker.v3.Worker;

/**
 * Message signaling that a given entity has been created or updated.
 */
public class EntityUpdatedMessage extends AbstractMessage {

    public EntityUpdatedMessage(String messageId, Long timestamp, String entityId, String entityType, String entitySource) {
        super(messageId, timestamp, entityId, entityType, entitySource);
    }

    @Override
    public boolean canCoalesce() {
        return true;
    }

    @Override
    public void dispatchTo(Worker worker) {
        worker.process(this);
    }
}
