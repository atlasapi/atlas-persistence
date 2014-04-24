package org.atlasapi.messaging.worker.v3;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessage.AdjacentRef;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metabroadcast.common.time.Timestamp;


public class ContentEquivalenceAssertionMessageConfiguration {

    @JsonCreator
    ContentEquivalenceAssertionMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("entityId") String entityId,
            @JsonProperty("entityType") String entityType,
            @JsonProperty("entitySource") String entitySource,
            @JsonProperty("adjacent") List<AdjacentRef> adjacent,
            @JsonProperty("sources") Set<String> sources) {
        
    }
    
}
