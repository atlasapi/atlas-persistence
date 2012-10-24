package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

public class LookupResolvingContentResolver implements ContentResolver {

    private final KnownTypeContentResolver knownTypeResolver;
    private final LookupEntryStore lookupResolver;

    public LookupResolvingContentResolver(KnownTypeContentResolver knownTypeResolver, LookupEntryStore lookupEntryStore) {
        this.knownTypeResolver = knownTypeResolver;
        this.lookupResolver = lookupEntryStore;
    }
    
    @Override
    public ResolvedContent findByCanonicalUris(Iterable<String> canonicalUris) {
        Iterable<LookupRef> lookupRefs = Iterables.transform(lookupResolver.entriesForCanonicalUris(canonicalUris), LookupEntry.TO_SELF);
        Iterable<LookupRef> resolvedLookups = Iterables.filter(lookupRefs, Predicates.notNull());
        ResolvedContent resolvedContent = knownTypeResolver.findByLookupRefs(resolvedLookups);
        return resolvedContent.copyWithAllRequestedUris(canonicalUris);
    }
}

