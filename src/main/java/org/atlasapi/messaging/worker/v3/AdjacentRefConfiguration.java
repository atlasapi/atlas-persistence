package org.atlasapi.messaging.worker.v3;

import org.atlasapi.media.entity.Publisher;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


public class AdjacentRefConfiguration {

    @JsonCreator
    AdjacentRefConfiguration(
            @JsonProperty("id") Long id,
            @JsonProperty("type") String type,
            @JsonProperty("source") Publisher source){
    }
    
}
