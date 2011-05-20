package org.atlasapi.persistence.lookup;

import java.util.List;

import org.atlasapi.application.ApplicationConfiguration;

public interface LookupResolver {
    
    List<Equivalent> lookup(String id, ApplicationConfiguration config);
    
    List<Equivalent> lookupGroup(String id, ApplicationConfiguration config);

}
