package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.LookupRef;
import com.google.common.collect.Iterables;

/**
 */
public class SimpleKnownTypeContentResolver implements KnownTypeContentResolver {

    private final ContentResolver resolver;

    public SimpleKnownTypeContentResolver(ContentResolver store) {
        this.resolver = store;
    }

    @Override
    public ResolvedContent findByLookupRefs(Iterable<LookupRef> lookupRefs) {
        return resolver.findByCanonicalUris(Iterables.transform(lookupRefs, LookupRef.TO_ID));
    }
}
