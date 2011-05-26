package org.atlasapi.persistence.lookup;

import java.util.List;

import org.atlasapi.application.ApplicationConfiguration;
import org.atlasapi.persistence.lookup.entry.Equivalent;

public interface LookupResolver {
    
    List<Equivalent> lookup(String id, ApplicationConfiguration config);
    
}
