package org.atlasapi.messaging.worker.v3;

import org.atlasapi.messaging.v3.BeginReplayMessage;
import org.atlasapi.messaging.v3.EndReplayMessage;
import org.atlasapi.messaging.v3.EntityUpdatedMessage;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage;
import org.atlasapi.messaging.v3.ReplayMessage;

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

    /**
     * Process a {@link org.ContentEquivalenceAssertionMessage.atlasapi.messaging.messaging.EquivalenceAssertionMessage}.
     */
    void process(ContentEquivalenceAssertionMessage equivalenceAssertionMessage);
}
