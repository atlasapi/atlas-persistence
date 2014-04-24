package org.atlasapi.messaging.worker.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class AdjacentRefConfiguration {

    @JsonCreator
    AdjacentRefConfiguration(
            @JsonProperty("id") String id,
            @JsonProperty("type") String type,
            @JsonProperty("source") String source){
    }
    
}
