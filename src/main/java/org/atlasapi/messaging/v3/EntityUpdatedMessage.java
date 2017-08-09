package org.atlasapi.messaging.v3;

import org.atlasapi.reporting.telescope.OwlTelescopeProxy;

import com.metabroadcast.common.queue.AbstractMessage;
import com.metabroadcast.common.time.Timestamp;

/**
 * Message signaling that a given entity has been created or updated.
 */
public class EntityUpdatedMessage extends AbstractMessage {

    private final String entityId;
    private final String entityType;
    private final String entitySource;
    private final OwlTelescopeProxy telescopeProxy;

    private EntityUpdatedMessage(
            String messageId,
            Timestamp timestamp,
            String entityId,
            String entityType,
            String entitySource,
            OwlTelescopeProxy telescopeProxy
    ) {
        super(messageId, timestamp);
        this.entityId = entityId;
        this.entitySource = entitySource;
        this.entityType = entityType;
        this.telescopeProxy = telescopeProxy;
    }

    public static EntityUpdatedMessage create(
            String messageId,
            Timestamp timestamp,
            String entityId,
            String entityType,
            String entitySource,
            OwlTelescopeProxy telescopeProxy
    ) {
        return new EntityUpdatedMessage(
                messageId,
                timestamp,
                entityId,
                entityType,
                entitySource,
                telescopeProxy
        );
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

    public OwlTelescopeProxy getTelescopeProxy() {
        return telescopeProxy;
    }
}
