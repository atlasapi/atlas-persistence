package org.atlasapi.persistence.content;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.Set;

public class LookupResolvingContentResolver implements ContentResolver {

    private final KnownTypeContentResolver knownTypeResolver;
    private final LookupEntryStore lookupResolver;

    public LookupResolvingContentResolver(KnownTypeContentResolver knownTypeResolver, LookupEntryStore mongoLookupEntryStore) {
        this.knownTypeResolver = knownTypeResolver;
        this.lookupResolver = mongoLookupEntryStore;
    }

    @Override
    public ResolvedContent findByCanonicalUris(Iterable<String> canonicalUris) {
        Set<String> dedupedUris = Sets.newHashSet(canonicalUris);
        Iterable<LookupRef> lookupRefs = Iterables.transform(lookupResolver.entriesForCanonicalUris(dedupedUris), LookupEntry.TO_SELF);
        ImmutableSet<LookupRef> resolvedLookups = ImmutableSet.copyOf(Iterables.filter(lookupRefs, Predicates.notNull()));
        ResolvedContent resolvedContent = knownTypeResolver.findByLookupRefs(resolvedLookups);
        return resolvedContent;//.copyWithAllRequestedUris(dedupedUris);
    }

    @Override
    public ResolvedContent findByIds(Iterable<Id> ids) {
        Set<Id> dedupedIds = Sets.newHashSet(ids);
        Iterable<LookupRef> lookupRefs = Iterables.transform(lookupResolver.entriesForIds(dedupedIds), LookupEntry.TO_SELF);
        ImmutableSet<LookupRef> resolvedLookups = ImmutableSet.copyOf(Iterables.filter(lookupRefs, Predicates.notNull()));
        ResolvedContent resolvedContent = knownTypeResolver.findByLookupRefs(resolvedLookups);
        return resolvedContent;
    }
}
