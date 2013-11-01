package org.atlasapi.persistence.content;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.atlasapi.application.ApplicationConfiguration;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.MorePredicates;

public class DefaultEquivalentContentResolver implements EquivalentContentResolver {

    private KnownTypeContentResolver contentResolver;
    private LookupEntryStore lookupResolver;
    private final Ordering<LookupRef> nullSafeRefById = Ordering.natural().onResultOf(LookupRef.TO_ID).nullsLast();

    public DefaultEquivalentContentResolver(KnownTypeContentResolver contentResolver, LookupEntryStore lookupResolver) {
        this.contentResolver = contentResolver;
        this.lookupResolver = lookupResolver;
    }
    
    @Override
    public EquivalentContent resolveUris(Iterable<String> uris, ApplicationConfiguration appConfig, Set<Annotation> activeAnnotations, boolean withAliases) {
        Iterable<LookupEntry> entries = lookupResolver.entriesForIdentifiers(uris, withAliases);
        return filterAndResolveEntries(entries, uris, appConfig);
    }
    
    @Override
    public EquivalentContent resolveIds(Iterable<Long> ids, ApplicationConfiguration appConfig, Set<Annotation> activeAnnotations) {
        Iterable<LookupEntry> entries = lookupResolver.entriesForIds(ids);
        Set<String> uris = Sets.newHashSet();
        for (LookupEntry entry : entries) {
            uris.add(entry.uri());
        }
        return filterAndResolveEntries(entries, uris, appConfig);
    }

    protected EquivalentContent filterAndResolveEntries(Iterable<LookupEntry> entries, Iterable<String> uris, ApplicationConfiguration appConfig) {
        Iterable<LookupEntry> selectedEntries = filterDisabledSources(entries, appConfig);
        
        if (Iterables.isEmpty(selectedEntries)) {
            return EquivalentContent.empty();
        }
        
        SetMultimap<String, LookupRef> uriToEquivs = byUri(subjsToEquivs(selectedEntries, appConfig));
        
        ResolvedContent resolvedContent = contentResolver.findByLookupRefs(ImmutableSet.copyOf(uriToEquivs.values()));
        
        EquivalentContent.Builder equivalentContent = EquivalentContent.builder();
        for (String uri : uris) {
            Set<LookupRef> equivUris = uriToEquivs.get(uri);
            Iterable<Content> content = equivContent(equivUris, resolvedContent);
            equivalentContent.putEquivalents(uri, content);
        }
        return equivalentContent.build();
    }

    private SetMultimap<String, LookupRef> byUri(SetMultimap<LookupRef, LookupRef> subjsToEquivs) {
        ImmutableSetMultimap.Builder<String, LookupRef> byUri = ImmutableSetMultimap.builder();
        for (Entry<LookupRef, Collection<LookupRef>> subjToEquivs : subjsToEquivs.asMap().entrySet()) {
            byUri.putAll(subjToEquivs.getKey().uri(), subjToEquivs.getValue());
        }
        return byUri.build();
    }

    protected Iterable<Content> equivContent(Set<LookupRef> equivUris, ResolvedContent resolved) {
        if (equivUris == null) {
            return ImmutableSet.of();
        }
        List<Identified> resolvedEquivs = resolved.getResolvedResults(Iterables.transform(equivUris,LookupRef.TO_URI));
        return Iterables.filter(resolvedEquivs, Content.class);
    }
    

    private SetMultimap<LookupRef, LookupRef> subjsToEquivs(Iterable<LookupEntry> resolved, ApplicationConfiguration appConfig) {
        Predicate<LookupRef> sourceFilter = MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, Predicates.in(appConfig.getEnabledSources()));

        SetMultimap<LookupRef, LookupRef> secondaryResolve = HashMultimap.create();
        
        ImmutableSetMultimap.Builder<LookupRef, LookupRef> subjsToEquivs = ImmutableSetMultimap.builder();
        for (LookupEntry entry : resolved) {
            Set<LookupRef> selectedEquivs = Sets.filter(entry.equivalents(), sourceFilter);
            if (appConfig.precedenceEnabled()) {
                //ensure only one from precedent
                LookupRef refToSave;
                if (isPrecedentSourceEntry(entry, appConfig)) {
                    refToSave = entry.lookupRef();
                } else {
                    refToSave = lowestIdFromPrecedentSource(selectedEquivs, appConfig);
                }
                if (refToSave != null) {
                    Iterable<LookupRef> toRemove = othersFromSourceOf(refToSave, selectedEquivs);
                    secondaryResolve.putAll(entry.lookupRef(), toRemove);
                }
            }
            subjsToEquivs.putAll(entry.lookupRef(), selectedEquivs);
        }
        return secondaryResolve.isEmpty() ? subjsToEquivs.build()
                                          : resolveAndFilter(secondaryResolve, subjsToEquivs.build());
    }

    private ImmutableSetMultimap<LookupRef, LookupRef> resolveAndFilter(
            SetMultimap<LookupRef, LookupRef> secondaryResolve, ImmutableSetMultimap<LookupRef, LookupRef> subjsToEquivs) {
        
        Map<LookupRef,LookupEntry> entriesToRemove = Maps.uniqueIndex(
            lookupResolver.entriesForCanonicalUris(Iterables.transform(ImmutableSet.copyOf(secondaryResolve.values()), LookupRef.TO_URI)),
            LookupEntry.TO_SELF
        );
        
        ImmutableSetMultimap.Builder<LookupRef, LookupRef> filtered
            = ImmutableSetMultimap.builder();
        
        for (Entry<LookupRef, Collection<LookupRef>> subjToEquivs : subjsToEquivs.asMap().entrySet()) {
            Set<LookupRef> filteredEquivs = Sets.newHashSet(subjToEquivs.getValue());
            Set<LookupRef> removalRefs = secondaryResolve.get(subjToEquivs.getKey());
            for (LookupRef equiv : subjToEquivs.getValue()) {
                if (removalRefs.contains(equiv)) {
                    LookupEntry entryToRemove = entriesToRemove.get(equiv);
                    if (entryToRemove != null) {
                        filteredEquivs.removeAll(Sets.union(entryToRemove.directEquivalents(), entryToRemove.explicitEquivalents()));
                    }
                }
            }
            //ensure we always get the thing we actually asked for.
            filteredEquivs.add(subjToEquivs.getKey());
            filtered.putAll(subjToEquivs.getKey(), filteredEquivs);
        }
        
        return filtered.build();
    }

    private LookupRef lowestIdFromPrecedentSource(Set<LookupRef> selectedEquivs,
            ApplicationConfiguration appConfig) {
        LookupRef lowestId = null;
        for (LookupRef lookupRef : Iterables.filter(selectedEquivs, fromSource(appConfig.precedence().get(0)))) {
            lowestId = nullSafeRefById.min(lowestId, lookupRef);
        }
        return lowestId;
    }

    private Iterable<LookupRef> othersFromSourceOf(LookupRef saveRef, Set<LookupRef> selectedEquivs) {
        return Iterables.filter(selectedEquivs, Predicates.and(Predicates.not(Predicates.equalTo(saveRef)),fromSource(saveRef.publisher())));
    }

    private Predicate<LookupRef> fromSource(Publisher src) {
        return MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, 
                Predicates.equalTo(src));
    }

    private boolean isPrecedentSourceEntry(LookupEntry entry, ApplicationConfiguration appConfig) {
        return appConfig.precedence().get(0).equals(entry.lookupRef().publisher());
    }

    private Iterable<LookupEntry> filterDisabledSources(Iterable<LookupEntry> entries, ApplicationConfiguration appConfig) {
        Predicate<Publisher> sourceFilter = Predicates.in(appConfig.getEnabledSources());
        Function<LookupEntry, Publisher> toSource = Functions.compose(LookupRef.TO_SOURCE, LookupEntry.TO_SELF);
        
        return Iterables.filter(entries, MorePredicates.transformingPredicate(toSource, sourceFilter));
    }

}
