package org.atlasapi.messaging.worker;

import org.atlasapi.messaging.BeginReplayMessage;
import org.atlasapi.messaging.EndReplayMessage;
import org.atlasapi.messaging.EntityUpdatedMessage;
import org.atlasapi.messaging.ReplayMessage;

/**
 * Base interface for workers that need to process {@link org.org.atlasapi.messaging.messaging.Message}s.
 */
public interface Worker {
    
    /**
     * Process a {@link org.org.atlasapi.messaging.messaging.EntityUpdatedMessage}.
     */
    void process(EntityUpdatedMessage message);
    
    /**
     * Process a {@link org.org.atlasapi.messaging.messaging.BeginReplayMessage}.
     */
    void process(BeginReplayMessage message);
    
    /**
     * Process a {@link org.org.atlasapi.messaging.messaging.EndReplayMessage}.
     */
    void process(EndReplayMessage message);
    
    /**
     * Process a {@link org.org.atlasapi.messaging.messaging.ReplayMessage}.
     */
    void process(ReplayMessage message);
}
