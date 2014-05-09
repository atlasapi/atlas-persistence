package org.atlasapi.messaging.v3;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;

/**
 * Message signaling that a given entity has been created or updated.
 */
public class EntityUpdatedMessage extends AbstractMessage {

    private final String entityId;
    private final String entityType;
    private final String entitySource;

    public EntityUpdatedMessage(String messageId, Timestamp timestamp, String entityId,
            String entityType, String entitySource) {
        super(messageId, timestamp);
        this.entityId = entityId;
        this.entitySource = entitySource;
        this.entityType = entityType;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getEntityType() {
        return entityType;
    }

    public String getEntitySource() {
        return entitySource;
    }

}
