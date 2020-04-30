package org.atlasapi.messaging.worker.v3;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.metabroadcast.common.time.Timestamp;

import java.util.Set;

public abstract class EquivalenceChangeMessageConfiguration {

    @JsonCreator
    EquivalenceChangeMessageConfiguration(
            @JsonProperty("messageId") String messageId,
            @JsonProperty("timestamp") Timestamp timestamp,
            @JsonProperty("subjectId") long subjectId,
            @JsonProperty("outgoingIdsAdded") Set<Long> outgoingIdsAdded,
            @JsonProperty("outgoingIdsRemoved") Set<Long> outgoingIdsRemoved,
            @JsonProperty("outgoingIdsUnchanged") Set<Long> outgoingIdsUnchanged,
            @JsonProperty("sources") Set<String> sources
    ) {
    }
}