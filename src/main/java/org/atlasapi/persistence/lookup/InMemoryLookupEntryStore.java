package org.atlasapi.persistence.lookup;

import java.util.Map;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

public class InMemoryLookupEntryStore implements LookupEntryStore {

    private Map<String,LookupEntry> store;

    public InMemoryLookupEntryStore() {
        store = Maps.newConcurrentMap();
    }
    
    @Override
    public void store(LookupEntry entry) {
        store.put(entry.id(), entry);
    }

    @Override
    public void store(Iterable<LookupEntry> entries) {
        store.putAll(Maps.uniqueIndex(entries, new Function<LookupEntry, String>() {
            @Override
            public String apply(LookupEntry input) {
                return input.id();
            }
        }));
    }
    
    @Override
    public LookupEntry entryFor(String identifier) {
        return store.get(identifier);
    }


}
