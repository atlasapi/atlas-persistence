package org.atlasapi.messaging.v3;

import com.metabroadcast.common.queue.Message;
import com.metabroadcast.common.time.Timestamp;

/**
 */
public abstract class AbstractMessage implements Message {

    private final String messageId;
    private final Timestamp timestamp;
    private final String entityId;
    private final String entityType;
    private final String entitySource;

    public AbstractMessage(String messageId, Timestamp timestamp, String entityId, String entityType, String entitySource) {
        this.messageId = messageId;
        this.timestamp = timestamp;
        this.entityId = entityId;
        this.entityType = entityType;
        this.entitySource = entitySource;
    }

    @Override
    public String getMessageId() {
        return messageId;
    }

    @Override
    public Timestamp getTimestamp() {
        return timestamp;
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

    public boolean canCoalesce() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Message) {
            Message other = (Message) o;
            return this.messageId.equals(other.getMessageId());
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return this.messageId.hashCode();
    }

    @Override
    public String toString() {
        return messageId;
    }
    
}
