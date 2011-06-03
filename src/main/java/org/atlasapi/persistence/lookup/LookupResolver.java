package org.atlasapi.persistence.lookup;

import java.util.List;

import org.atlasapi.application.ApplicationConfiguration;
import org.atlasapi.media.entity.LookupRef;

public interface LookupResolver {
    
    List<LookupRef> lookup(String id, ApplicationConfiguration config);
    
}
