package org.atlasapi.persistence.content.elasticsearch.schema;

import java.util.Date;

/**
 */
public class ESVersion extends ESObject {
    
    public final static String CHANNEL = "channel";
    public final static String TRANSMISSION_TIME = "transmissionTime";
    public final static String TRANSMISSION_END_TIME = "transmissionEndTime";
    
    public ESVersion channel(String channel) {
        properties.put(CHANNEL, channel);
        return this;
    }
    
    public ESVersion transmissionTime(Date transmissionTime) {
        properties.put(TRANSMISSION_TIME, transmissionTime);
        return this;
    }
    
    public ESVersion transmissionEndTime(Date transmissionEndTime) {
        properties.put(TRANSMISSION_END_TIME, transmissionEndTime);
        return this;
    }
}
