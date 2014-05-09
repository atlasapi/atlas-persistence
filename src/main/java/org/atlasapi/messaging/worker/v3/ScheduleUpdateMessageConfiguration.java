package org.atlasapi.messaging.worker.v3;


import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metabroadcast.common.time.Timestamp;

public class ScheduleUpdateMessageConfiguration {

    
    @JsonCreator
    public ScheduleUpdateMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("source") String source,
            @JsonProperty("channel") String channel,
            @JsonProperty("updateStart") DateTime updateStart,
            @JsonProperty("updateEnd") DateTime updateEnd
        ) {
    }
    
}
