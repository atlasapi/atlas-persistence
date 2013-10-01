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
    public ResolvedContent findByCanonicalUris(Iterable<String> canonicalUris) {
        Iterable<LookupEntry> entriesForCanonicalUris = lookupResolver.entriesForCanonicalUris(canonicalUris);
        return resolveLookupEntries(canonicalUris, entriesForCanonicalUris);
    }

    @Override
    public ResolvedContent findByUris(Iterable<String> uris) {
        Iterable<LookupEntry> entriesForIdentifiers = lookupResolver.entriesForIdentifiers(uris, true);
        return resolveLookupEntries(uris, entriesForIdentifiers);
    }
    
    private ResolvedContent resolveLookupEntries(Iterable<String> uris,
            Iterable<LookupEntry> entriesForCanonicalUris) {
        Iterable<LookupRef> lookupRefs = Iterables.transform(entriesForCanonicalUris, LookupEntry.TO_SELF);
        ImmutableSet<LookupRef> resolvedLookups = ImmutableSet.copyOf(Iterables.filter(lookupRefs, Predicates.notNull()));
        ResolvedContent resolvedContent = knownTypeResolver.findByLookupRefs(resolvedLookups);
        return resolvedContent.copyWithAllRequestedUris(uris);
    }

}

