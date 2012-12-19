package org.atlasapi.persistence.lookup;

import java.util.Map;

import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Functions;
import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class InMemoryLookupEntryStore implements LookupEntryStore {

    private Map<String,LookupEntry> uriStore;
    private Map<Long,LookupEntry> idStore;
    private Multimap<String,LookupEntry> identifierStore;

    public InMemoryLookupEntryStore() {
        uriStore = Maps.newConcurrentMap();
        idStore = Maps.newConcurrentMap();
        identifierStore = ArrayListMultimap.create();
    }
    
    @Override
    public void store(LookupEntry entry) {
        uriStore.put(entry.uri(), entry);
        if (entry.id() != null) {
            idStore.put(entry.id(), entry);
        }
        for (String alias : entry.aliases()) {
            identifierStore.put(alias, entry);
        }
    }
    
    @Override
    public Iterable<LookupEntry> entriesForCanonicalUris(Iterable<String> uris) {
        return Iterables.filter(Iterables.transform(uris, Functions.forMap(uriStore, null)), Predicates.notNull());
    }

    @Override
    public Iterable<LookupEntry> entriesForIds(Iterable<Long> ids) {
        return Iterables.filter(Iterables.transform(ids, Functions.forMap(idStore, null)), Predicates.notNull());
    }

    @Override
    public Iterable<LookupEntry> entriesForIdentifiers(Iterable<String> identifiers, boolean useAliases) {
        return Iterables.concat(Iterables.filter(Iterables.transform(identifiers, Functions.forMap(identifierStore.asMap(),null)),Predicates.notNull()));
    }
}
