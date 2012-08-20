package org.atlasapi.persistence.content.elasticsearch.schema;

import com.google.common.collect.Iterables;
import java.util.Collection;

/**
 */
public class ESItem extends ESObject{

    public final static String TYPE = "item";
    //
    public final static String URI = "uri";
    public final static String PUBLISHER = "publisher";
    public final static String BROADCASTS = "broadcasts";
    public final static String TOPICS = "topics";
    
    public ESItem uri(String uri) {
        properties.put(URI, uri);
        return this;
    }
    
    public ESItem publisher(String publisher) {
        properties.put(PUBLISHER, publisher);
        return this;
    }
    
    public ESItem broadcasts(Collection<ESBroadcast> broadcasts) {
        properties.put(BROADCASTS, Iterables.transform(broadcasts, TO_MAP));
        return this;
    }
    
    public ESItem topics(Collection<ESTopic> topics) {
        properties.put(TOPICS, Iterables.transform(topics, TO_MAP));
        return this;
    }
}
