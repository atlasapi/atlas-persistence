package org.atlasapi.messaging.v3;

import org.atlasapi.messaging.worker.v3.Worker;

/**
 * Message signaling the end of a messages replay.
 */
public class EndReplayMessage extends AbstractMessage {

    public EndReplayMessage(String messageId, Long timestamp, String entityId, String entityType, String entitySource) {
        super(messageId, timestamp, entityId, entityType, entitySource);
    }

    @Override
    public void dispatchTo(Worker worker) {
        worker.process(this);
    }
}
