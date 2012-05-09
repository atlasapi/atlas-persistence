package org.atlasapi.persistence.content.cassandra;

import com.google.common.collect.Iterables;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupRef;

/**
 */
public class CassandraKnownTypeContentResolver implements KnownTypeContentResolver {

    private final CassandraContentStore store;

    public CassandraKnownTypeContentResolver(CassandraContentStore store) {
        this.store = store;
    }

    @Override
    public ResolvedContent findByLookupRefs(Iterable<LookupRef> lookupRefs) {
        return store.findByCanonicalUris(Iterables.transform(lookupRefs, LookupRef.TO_ID));
    }
}
