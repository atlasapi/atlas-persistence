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
    public final static String VERSIONS = "versions";
    
    public ESItem uri(String uri) {
        properties.put(URI, uri);
        return this;
    }
    
    public ESItem publisher(String publisher) {
        properties.put(PUBLISHER, publisher);
        return this;
    }
    
    public ESItem versions(Collection<ESVersion> versions) {
        properties.put(VERSIONS, Iterables.transform(versions, TO_MAP));
        return this;
    }
}
