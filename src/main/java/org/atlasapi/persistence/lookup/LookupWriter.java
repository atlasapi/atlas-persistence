package org.atlasapi.persistence.lookup;

import org.atlasapi.media.entity.Described;

public interface LookupWriter {

    <T extends Described> void writeLookup(T subject, Iterable<T> equivalents);
    
}
