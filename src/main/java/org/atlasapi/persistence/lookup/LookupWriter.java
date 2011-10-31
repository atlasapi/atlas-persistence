package org.atlasapi.persistence.lookup;

import org.atlasapi.media.entity.Content;

public interface LookupWriter {

    <T extends Content> void writeLookup(T subject, Iterable<T> equivalents);
    
}
