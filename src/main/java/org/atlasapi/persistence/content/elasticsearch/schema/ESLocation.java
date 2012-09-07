package org.atlasapi.persistence.content.elasticsearch.schema;

import java.util.Date;

/**
 */
public class ESLocation extends ESObject {
    
    public final static String AVAILABILITY_TIME = "availabilityTime";
    public final static String AVAILABILITY_END_TIME = "availabilityEndTime";
    
    
    public ESLocation availabilityTime(Date availabilityTime) {
        properties.put(AVAILABILITY_TIME, availabilityTime);
        return this;
    }
    
    public ESLocation availabilityEndTime(Date availabilityEndTime) {
        properties.put(AVAILABILITY_END_TIME, availabilityEndTime);
        return this;
    }
    
}
