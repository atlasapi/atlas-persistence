package org.atlasapi.persistence.lookup;

public interface LookupEntryStore {

    void store(LookupEntry entry);
    
    void store(Iterable<LookupEntry> entries);
    
    LookupEntry entryFor(String identifier);
    
}
