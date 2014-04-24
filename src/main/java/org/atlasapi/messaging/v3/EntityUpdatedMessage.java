package org.atlasapi.messaging.v3;

import com.metabroadcast.common.time.Timestamp;

/**
 * Message signaling that a given entity has been created or updated.
 */
public class EntityUpdatedMessage extends AbstractMessage {

    public EntityUpdatedMessage(String messageId, Timestamp timestamp, String entityId, String entityType, String entitySource) {
        super(messageId, timestamp, entityId, entityType, entitySource);
    }

    @Override
    public boolean canCoalesce() {
        return true;
    }

}
