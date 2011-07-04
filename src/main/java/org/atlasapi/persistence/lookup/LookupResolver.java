package org.atlasapi.persistence.lookup;

import java.util.List;

import org.atlasapi.media.entity.LookupRef;

public interface LookupResolver {
    
    LookupRef lookup(String id);
    
    List<LookupRef> equivalentsFor(String id);
    
}
