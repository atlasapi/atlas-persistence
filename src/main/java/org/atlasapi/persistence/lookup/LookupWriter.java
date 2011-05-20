package org.atlasapi.persistence.lookup;

import java.util.Set;

import org.atlasapi.media.entity.Described;

public interface LookupWriter {

    void writeLookup(Described subject, Set<Described> equivalents);
    
}
