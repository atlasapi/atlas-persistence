package org.atlasapi.messaging.worker.v3;

import org.atlasapi.messaging.v3.Message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public abstract class ReplayMessageConfiguration {

    @JsonCreator
    ReplayMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Long timestamp,
            @JsonProperty("entityId") String entityId,
            @JsonProperty("entityType") String entityType,
            @JsonProperty("entitySource") String entitySource,
            @JsonProperty("original") Message original) {
        
    }
    
}