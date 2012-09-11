package org.atlasapi.persistence.content.elasticsearch.schema;

import java.util.Date;

/**
 */
public class ESBroadcast extends ESObject {
    
    public final static String ID = "id";
    public final static String CHANNEL = "channel";
    public final static String TRANSMISSION_TIME = "transmissionTime";
    public final static String TRANSMISSION_END_TIME = "transmissionEndTime";
    public final static String TRANSMISSION_TIME_IN_MILLIS = "transmissionTimeInMillis";
    public final static String REPEAT = "repeat";
    
    public ESBroadcast id(String id) {
        properties.put(ID, id);
        return this;
    }
    
    public ESBroadcast channel(String channel) {
        properties.put(CHANNEL, channel);
        return this;
    }
    
    public ESBroadcast transmissionTime(Date transmissionTime) {
        properties.put(TRANSMISSION_TIME, transmissionTime);
        return this;
    }
    
    public ESBroadcast transmissionEndTime(Date transmissionEndTime) {
        properties.put(TRANSMISSION_END_TIME, transmissionEndTime);
        return this;
    }
    
    public ESBroadcast transmissionTimeInMillis(Long transmissionTimeInMillis) {
        properties.put(TRANSMISSION_TIME_IN_MILLIS, transmissionTimeInMillis);
        return this;
    }
    
    public ESBroadcast repeat(Boolean repeat) {
        properties.put(REPEAT, repeat);
        return this;
    }
}
