package org.atlasapi.persistence.lookup;

import java.util.List;

import org.atlasapi.application.ApplicationConfiguration;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.collect.ImmutableList;

public class BasicLookupResolver implements LookupResolver {

    private final LookupEntryStore lookupStore;

    public BasicLookupResolver(LookupEntryStore lookupStore) {
        this.lookupStore = lookupStore;
    }
    
    @Override
    public List<LookupRef> lookup(String id, ApplicationConfiguration config) {
        LookupEntry entry = lookupStore.entryFor(id);
        return entry != null ? entry.equivalents() : ImmutableList.<LookupRef>of();
    }
    
}
