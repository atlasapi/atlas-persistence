package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class LookupResolvingContentResolver implements ContentResolver {

    private final KnownTypeContentResolver knownTypeResolver;
    private final LookupEntryStore lookupResolver;

    public LookupResolvingContentResolver(KnownTypeContentResolver knownTypeResolver, LookupEntryStore mongoLookupEntryStore) {
        this.knownTypeResolver = knownTypeResolver;
        this.lookupResolver = mongoLookupEntryStore;
    }
    
    @Override
    public ResolvedContent findByCanonicalUris(Iterable<? extends String> canonicalUris) {
        Iterable<LookupEntry> entriesForCanonicalUris = lookupResolver.entriesForCanonicalUris(canonicalUris);
        return resolveLookupEntries(entriesForCanonicalUris).copyWithAllRequestedUris(canonicalUris);
    }

    @Override
    public ResolvedContent findByUris(Iterable<String> uris) {
        Iterable<LookupEntry> entriesForIdentifiers = lookupResolver.entriesForIdentifiers(uris, true);
        return resolveLookupEntries(entriesForIdentifiers);
    }
    
    private ResolvedContent resolveLookupEntries(Iterable<LookupEntry> entriesForCanonicalUris) {
        Iterable<LookupRef> lookupRefs = Iterables.transform(entriesForCanonicalUris, LookupEntry.TO_SELF);
        ImmutableSet<LookupRef> resolvedLookups = ImmutableSet.copyOf(Iterables.filter(lookupRefs, Predicates.notNull()));
        return knownTypeResolver.findByLookupRefs(resolvedLookups);
    }

}

