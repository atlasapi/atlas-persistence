package org.atlasapi.persistence.lookup;

import java.util.Set;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

public class TransitiveLookupWriter implements LookupWriter {
    
    private final static LookupWriter NO_OP = new LookupWriter() {
        
        @Override
        public void writeLookup(ContentRef subjectUri, Iterable<ContentRef> equivalentUris,
                Set<Publisher> publishers) {
            //no-op
        }
    };
    
    public static LookupWriter explicitTransitiveLookupWriter(LookupEntryStore entryStore) {
        return NO_OP;
    }
    
    public static LookupWriter generatedTransitiveLookupWriter(LookupEntryStore entryStore) {
        return NO_OP;
    }
    
    
    private TransitiveLookupWriter() { }
    
    @Override
    public void writeLookup(ContentRef subjectUri, Iterable<ContentRef> equivalentUris,
            Set<Publisher> publishers) {
    }
}
