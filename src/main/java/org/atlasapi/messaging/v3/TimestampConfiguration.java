package org.atlasapi.messaging.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TimestampConfiguration {

    @JsonCreator
    public TimestampConfiguration(@JsonProperty("millis") Long millis) {
    }
    
}
