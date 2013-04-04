package org.atlasapi.persistence.content;

import org.atlasapi.equiv.EquivalenceRef;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

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
        Iterable<LookupEntry> lookupEntries = lookupResolver.entriesForCanonicalUris(dedupedUris);
        Iterable<LookupRef> lookupRefs = Iterables.transform(lookupEntries, LookupEntry.TO_SELF);
        ImmutableSet<LookupRef> resolvedLookups = ImmutableSet.copyOf(Iterables.filter(lookupRefs, Predicates.notNull()));
        ResolvedContent resolvedContent = knownTypeResolver.findByLookupRefs(resolvedLookups);
        
        return setEquivalenceRefs(resolvedContent, lookupEntries);//.copyWithAllRequestedUris(dedupedUris);
    }

    @Override
    public ResolvedContent findByIds(Iterable<Id> ids) {
        Set<Id> dedupedIds = Sets.newHashSet(ids);
        Iterable<LookupEntry> lookupEntries = lookupResolver.entriesForIds(dedupedIds);
        Iterable<LookupRef> lookupRefs = Iterables.transform(lookupEntries, LookupEntry.TO_SELF);
        ImmutableSet<LookupRef> resolvedLookups = ImmutableSet.copyOf(Iterables.filter(lookupRefs, Predicates.notNull()));
        ResolvedContent resolvedContent = knownTypeResolver.findByLookupRefs(resolvedLookups);
        return setEquivalenceRefs(resolvedContent, lookupEntries);
    }

    private ResolvedContent setEquivalenceRefs(ResolvedContent resolvedContent,
            Iterable<LookupEntry> lookupEntries) {
        Map<String, LookupEntry> entryIndex = Maps.uniqueIndex(lookupEntries, LookupEntry.TO_URI);
        for (Identified resolved : resolvedContent.getAllResolvedResults()) {
            resolved.setEquivalentTo(toEquivRefs(resolved, entryIndex.get(resolved.getCanonicalUri())));
        }
        return resolvedContent;
    }

    private Set<EquivalenceRef> toEquivRefs(final Identified resolved, LookupEntry lookupEntry) {
        return ImmutableSet.copyOf(Iterables.transform(Sets.filter(lookupEntry.explicitEquivalents(),
                new Predicate<LookupRef>() {
                    @Override
                    public boolean apply(@Nullable LookupRef input) {
                        return !input.id().equals(resolved.getId());
                    }
                }),
                new Function<LookupRef, EquivalenceRef>() {

                    @Override
                    public EquivalenceRef apply(@Nullable LookupRef input) {
                        return new EquivalenceRef(input.id(), input.publisher());
                    }
                }));
    }
}
