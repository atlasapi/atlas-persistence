package org.atlasapi.persistence.lookup;

import org.atlasapi.media.entity.Described;

public interface NewLookupWriter {
    
    void ensureLookup(Described described);

}
