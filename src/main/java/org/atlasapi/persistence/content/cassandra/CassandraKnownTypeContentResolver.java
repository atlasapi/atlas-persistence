package org.atlasapi.persistence.content.cassandra;

import org.atlasapi.media.content.LookupRef;
import org.atlasapi.media.content.ResolvedContent;
import org.atlasapi.media.content.util.KnownTypeContentResolver;

import com.google.common.collect.Iterables;

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
