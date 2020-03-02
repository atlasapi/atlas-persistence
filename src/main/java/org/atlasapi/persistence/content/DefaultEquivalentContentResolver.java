package org.atlasapi.persistence.content;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.base.MorePredicates;
import com.metabroadcast.common.stream.MoreCollectors;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.atlasapi.output.Annotation.RESPECT_API_KEY_FOR_EQUIV_LIST;

public class DefaultEquivalentContentResolver implements EquivalentContentResolver {

    private KnownTypeContentResolver contentResolver;
    private LookupEntryStore lookupResolver;
    private final Ordering<LookupRef> nullSafeRefById = Ordering.natural().onResultOf(LookupRef.TO_ID).nullsLast();
    private final Logger log = LoggerFactory.getLogger(DefaultEquivalentContentResolver.class);

    public DefaultEquivalentContentResolver(
            KnownTypeContentResolver contentResolver,
            LookupEntryStore lookupResolver
    ) {
        this.contentResolver = contentResolver;
        this.lookupResolver = lookupResolver;
    }
    
    @Override
    public EquivalentContent resolveUris(
            Iterable<String> uris,
            Application application,
            Set<Annotation> activeAnnotations,
            boolean withAliases
    ) {
        Iterable<LookupEntry> entries = lookupResolver.entriesForIdentifiers(uris, withAliases);
        return filterAndResolveEntries(ImmutableSet.copyOf(entries), uris, application, activeAnnotations);
    }
    
    @Override
    public EquivalentContent resolveIds(
            Iterable<Long> ids,
            Application application,
            Set<Annotation> activeAnnotations
    ) {
        Iterable<LookupEntry> entries = lookupResolver.entriesForIds(ids);
        Set<String> uris = Sets.newHashSet();
        for (LookupEntry entry : entries) {
            uris.add(entry.uri());
        }
        return filterAndResolveEntries(ImmutableSet.copyOf(entries), uris, application, activeAnnotations);
    }
    
    @Override
    public EquivalentContent resolveAliases(
            Optional<String> namespace,
            Iterable<String> values,
            Application application,
            Set<Annotation> activeAnnotations
    ) {
        Iterable<LookupEntry> entries = lookupResolver.entriesForAliases(namespace, values);
        Set<String> uris = Sets.newHashSet();
        for (LookupEntry entry : entries) {
            uris.add(entry.uri());
        }
        return filterAndResolveEntries(ImmutableSet.copyOf(entries), uris, application, activeAnnotations);
    }

    protected EquivalentContent filterAndResolveEntries(
            Set<LookupEntry> entries,
            Iterable<String> uris,
            Application application,
            Set<Annotation> activeAnnotations
    ) {
        if (Iterables.isEmpty(entries)) {
            return EquivalentContent.empty();
        }
        
        SetMultimap<String, LookupRef> uriToEquivs = byUri(subjsToEquivs(entries, application, activeAnnotations));
        
        ImmutableSet<LookupRef> refs = ImmutableSet.copyOf(uriToEquivs.values());
        if (refs.isEmpty()) {
            return EquivalentContent.empty();
        }
        
        Map<String, LookupEntry> entryIndex = Maps.uniqueIndex(entries, LookupEntry.TO_ID);
        ResolvedContent resolvedContent = contentResolver.findByLookupRefs(refs);
        
        EquivalentContent.Builder equivalentContent = EquivalentContent.builder();
        for (String uri : uris) {
            Set<LookupRef> equivRefs = uriToEquivs.get(uri);
            Iterable<Content> contents = equivContent(equivRefs, resolvedContent);
            LookupEntry entry = entryIndex.get(uri);
            if (entry != null) {
                Set<LookupRef> allRefs = equivRefs(contents, entry, application);
                for (Content content : contents) {
                    content.setEquivalentTo(allRefs);
                }
            }
            equivalentContent.putEquivalents(uri, contents);
        }
        return equivalentContent.build();
    }

    private Set<LookupRef> equivRefs(Iterable<Content> contents, LookupEntry entry,
            Application application) {
        Iterable<LookupRef> enabled = Iterables.transform(contents, LookupRef.FROM_DESCRIBED);
        Set<LookupRef> disabled = removeEnabledSources(entry.equivalents(), application);
        return Sets.union(ImmutableSet.copyOf(enabled), disabled);
    }

    private Set<LookupRef> removeEnabledSources(Set<LookupRef> equivalents,
            Application application) {
        Predicate<Publisher> isDisabled = Predicates.not(Predicates.in(application.getConfiguration().getEnabledReadSources()));
        return ImmutableSet.copyOf(Iterables.filter(equivalents, 
            MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, isDisabled)));
    }

    private SetMultimap<String, LookupRef> byUri(SetMultimap<LookupEntry, LookupRef> subjsToEquivs) {
        ImmutableSetMultimap.Builder<String, LookupRef> byUri = ImmutableSetMultimap.builder();
        for (Entry<LookupEntry, Collection<LookupRef>> subjToEquivs : subjsToEquivs.asMap().entrySet()) {
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
    

    protected SetMultimap<LookupEntry, LookupRef> subjsToEquivs(
            Iterable<LookupEntry> resolved,
            Application application,
            Set<Annotation> activeAnnotations
    ) {
        java.util.function.Predicate<LookupRef> sourceFilter = MorePredicates.transformingPredicate(
                LookupRef.TO_SOURCE,
                Predicates.in(application.getConfiguration().getEnabledReadSources()))::apply;

        SetMultimap<LookupRef, LookupRef> secondaryResolve = HashMultimap.create();
        
        ImmutableSetMultimap.Builder<LookupEntry, LookupRef> subjsToEquivs = ImmutableSetMultimap.builder();
        for (LookupEntry entry : resolved) {
            Set<LookupRef> selectedEquivs;

            // Keep in mind that the code will later reduce that to a single thing from the
            // precedent source, this means that even though RESPECT_API_KEY_FOR_EQUIV_LIST
            // is here for consistency, if you do not include the
            // ALLOW_MULTIPLE_FROM_SAME_PUBLISHER_IN_EQUIV_LIST annotation, results
            // will be weird.
            selectedEquivs = getEquivList(activeAnnotations, sourceFilter, entry);

            if (application.getConfiguration().isPrecedenceEnabled()) {
                //ensure only one from precedent
                LookupRef refToSave;
                if (isPrecedentSourceEntry(entry, application)) {
                    refToSave = entry.lookupRef();
                } else {
                    refToSave = lowestIdFromPrecedentSource(selectedEquivs, application);
                }
                if (refToSave != null) {
                    Iterable<LookupRef> toRemove = othersFromSourceOf(refToSave, selectedEquivs);
                    secondaryResolve.putAll(entry.lookupRef(), toRemove);
                }
            }
            subjsToEquivs.putAll(entry, selectedEquivs);
        }
        return secondaryResolve.isEmpty() ? subjsToEquivs.build()
                                          : resolveAndFilter(secondaryResolve, subjsToEquivs.build(), sourceFilter);
    }

    protected Set<LookupRef> getEquivList(Set<Annotation> activeAnnotations,
            java.util.function.Predicate<LookupRef> sourceFilter, LookupEntry entry) {
        Set<LookupRef> selectedEquivs;
        if (activeAnnotations.contains(RESPECT_API_KEY_FOR_EQUIV_LIST)) {
            selectedEquivs = getEquivSetByFollowingLinks(entry, sourceFilter);
        } else {
            selectedEquivs = Sets.filter(entry.equivalents(), sourceFilter::test);
        }
        return selectedEquivs;
    }

    // Steps through and filters the explicit equiv set to avoid collecting any
    // transitive equivs from sources not allowed from the api key. This is a very expensive call
    // as it needs to resolve the whole equiv set.
    @VisibleForTesting
    Set<LookupRef> getEquivSetByFollowingLinks(
            LookupEntry startingContent,
            java.util.function.Predicate<LookupRef> sourceFilter
    ) {

        int linkDepth = 0; // for monitoring
        int resolutionsRequired = 0;

        Map<LookupRef, Boolean> collectedEquivsAndPublishedState = new HashMap<>();

        Iterable<LookupEntry> resolvedEntries = ImmutableList.of(startingContent);

        boolean equivsExhausted = false;
        while (!equivsExhausted) {
            for(LookupEntry entry : resolvedEntries) {
                collectedEquivsAndPublishedState.putIfAbsent(entry.lookupRef(), entry.activelyPublished());
            }

            //get links to resolve from the next equiv sets, until there are no more links to follow.
            Set<LookupRef> nextLinks = StreamSupport.stream(resolvedEntries.spliterator(), false)
                    .filter(LookupEntry::activelyPublished)
                    .flatMap(entry -> entry.getNeighbours().stream())
                    .distinct()
                    .filter(sourceFilter)
                    .filter(ref -> !collectedEquivsAndPublishedState.containsKey(ref))
                    .collect(MoreCollectors.toImmutableSet());

            if(nextLinks.isEmpty()) {
                equivsExhausted = true; //no more links to follow
            } else {
                resolutionsRequired += nextLinks.size();
                linkDepth++;
                //then resolve them
                resolvedEntries = lookupResolver.entriesForCanonicalUris(
                        Iterables.transform(nextLinks, LookupRef::uri)
                );
            }
        }

        //arbitrary warning, as we do not currently know the actual impact of this.
        if(linkDepth >= 5 || resolutionsRequired >= 20) {
            log.warn("Large set has to be resolved in order to calculate the owl equiv set that"
                     + " respects the API key, id:" + startingContent.id()
                     + ", linkDepth:" + linkDepth
                     + ", totalResolutions:" + resolutionsRequired
            );
        }

        return collectedEquivsAndPublishedState.keySet().stream()
                .filter(collectedEquivsAndPublishedState::get) //filter to just published
                .collect(MoreCollectors.toImmutableSet());
    }

    private ImmutableSetMultimap<LookupEntry, LookupRef> resolveAndFilter(
            SetMultimap<LookupRef, LookupRef> secondaryResolve,
            ImmutableSetMultimap<LookupEntry, LookupRef> subjsToEquivs,
            java.util.function.Predicate<LookupRef> sourceFilter
    ) {
        
        Map<LookupRef,LookupEntry> entriesToRemove = Maps.uniqueIndex(
            lookupResolver.entriesForCanonicalUris(Iterables.transform(ImmutableSet.copyOf(secondaryResolve.values()), LookupRef.TO_URI)),
            LookupEntry.TO_SELF
        );
        
        ImmutableSetMultimap.Builder<LookupEntry, LookupRef> filtered
            = ImmutableSetMultimap.builder();
        
        for (Entry<LookupEntry, Collection<LookupRef>> subjToEquivs : subjsToEquivs.asMap().entrySet()) {
            Set<LookupRef> filteredEquivs = Sets.newHashSet(subjToEquivs.getValue());
            Set<LookupRef> removalRefs = secondaryResolve.get(subjToEquivs.getKey().lookupRef());
            //remove all the adjacent of the refs to remove. 
            for (LookupRef equiv : subjToEquivs.getValue()) {
                if (removalRefs.contains(equiv)) {
                    LookupEntry entryToRemove = entriesToRemove.get(equiv);
                    if (entryToRemove != null) {
                        filteredEquivs.removeAll(entryToRemove.getNeighbours());
                    }
                }
            }
            //ensure we always get the thing we actually asked for.
            if (sourceFilter.test(subjToEquivs.getKey().lookupRef())) {
                filteredEquivs.add(subjToEquivs.getKey().lookupRef());
            }
            //and its exclusive enabled adjacent.
            filteredEquivs.addAll(Sets.difference(
                Sets.filter(subjToEquivs.getKey().getNeighbours(), sourceFilter::test),
                secondaryResolve.get(subjToEquivs.getKey().lookupRef())
            ));
            filtered.putAll(subjToEquivs.getKey(), filteredEquivs);
        }
        
        return filtered.build();
    }

    private LookupRef lowestIdFromPrecedentSource(
            Set<LookupRef> selectedEquivs,
            Application application
    ) {
        LookupRef lowestId = null;

        Publisher publisher = application.getConfiguration()
                .getReadPrecedenceOrdering()
                .sortedCopy(application.getConfiguration().getEnabledReadSources())
                .get(0);

        Iterable<LookupRef> lookupRefs = selectedEquivs.stream()
                .filter(fromSource(publisher)::apply)
                .collect(Collectors.toList());

        for (LookupRef lookupRef : lookupRefs) {
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

    private boolean isPrecedentSourceEntry(LookupEntry entry, Application application) {
        return application.getConfiguration()
                .getReadPrecedenceOrdering()
                .sortedCopy(application.getConfiguration().getEnabledReadSources())
                .get(0)
                .equals(entry.lookupRef().publisher());
    }

}
