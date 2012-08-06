package org.atlasapi.persistence.lookup;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static org.atlasapi.media.entity.Identified.TO_URI;
import static org.atlasapi.media.entity.LookupRef.TO_ID;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;

public class TransitiveLookupWriter implements LookupWriter {
    
    private static final Logger log = LoggerFactory.getLogger(TransitiveLookupWriter.class);
    private static final int maxSetSize = 150;
    
    private final LookupEntryStore entryStore;
    private final boolean explicit;
    
    public static TransitiveLookupWriter explicitTransitiveLookupWriter(LookupEntryStore entryStore) {
        return new TransitiveLookupWriter(entryStore, true);
    }
    
    public static TransitiveLookupWriter generatedTransitiveLookupWriter(LookupEntryStore entryStore) {
        return new TransitiveLookupWriter(entryStore, false);
    }

    private TransitiveLookupWriter(LookupEntryStore entryStore, boolean explicit) {
        this.entryStore = entryStore;
        this.explicit = explicit;
    }

    @Override
    public <T extends Content> void writeLookup(T subject, Iterable<T> equivalents, Set<Publisher> publishers) {
        writeLookup(subject.getCanonicalUri(), Iterables.transform(filterContentPublishers(equivalents, publishers), TO_URI), publishers);
    }
    
    public void writeLookup(final String subjectUri, Iterable<String> equivalentUris, final Set<Publisher> publishers) {
        Preconditions.checkArgument(emptyToNull(subjectUri) != null, "Subject URI was null/empty");
        
        try {
            attemptWriteLookup(subjectUri, equivalentUris, publishers);
        } catch (Throwable e) {
            throw new RuntimeException(subjectUri, e);
        }
    }

    private void attemptWriteLookup(final String subjectUri, Iterable<String> equivalentUris, final Set<Publisher> publishers) {
        //canonical URIs of subject and directEquivalents
        final Set<String> canonUris = ImmutableSet.<String>builder().add(subjectUri).addAll(equivalentUris).build();

        //entry for the subject.
        LookupEntry subjectEntry = get(subjectUri);
        
        ImmutableSet<String> currentEquivalents = ImmutableSet.copyOf(transform(relevantEquivalents(subjectEntry),TO_ID));
        if(currentEquivalents.equals(canonUris)) {
            return;
        }
        log.trace("Equivalence change: {} -> {}", currentEquivalents, canonUris);
        
        Set<LookupEntry> equivEntries = entriesFor(equivalentUris);

        //Pull the current transitive closures for the directly equivalent parameters.
        Map<LookupRef, LookupEntry> lookups = transitiveClosure(ImmutableSet.copyOf(Iterables.concat(ImmutableSet.of(subjectEntry), equivEntries)));
        
        if(lookups.size() > maxSetSize) {
            log.info("Transitive set too large: {} {}", subjectUri, lookups.size());
            return;
        }
        
        final ImmutableSet<LookupRef> subjectRef = ImmutableSet.of(subjectEntry.lookupRef());
        //Update the direct equivalents for all the lookups.
        lookups = Maps.newHashMap(Maps.transformValues(lookups, new Function<LookupEntry, LookupEntry>() {
                @Override
                public LookupEntry apply(LookupEntry entry) {
                    // Only modify direct equivalents of entries in the
                    // transitive closure of publishers that are argued
                    if (!publishers.contains(entry.lookupRef().publisher())) {
                        return entry;
                    }
                    SetView<LookupRef> updatedNeighbours;
                    if (canonUris.contains(entry.uri())) {
                        updatedNeighbours = Sets.union(relevantEquivalents(entry), subjectRef);
                    } else {
                        updatedNeighbours = Sets.difference(relevantEquivalents(entry), subjectRef);
                    }
                    return explicit ? entry.copyWithExplicitEquivalents(updatedNeighbours) 
                                    : entry.copyWithDirectEquivalents(updatedNeighbours);
                }
        }));
        
        /* Update the subject content entry. Included:
         *  refs of publishers not argued.
         *  refs for all content argued as equivalent.
         */
        Iterable<LookupRef> neighbours = neighbours(publishers, subjectEntry, equivEntries);
        lookups.put(subjectEntry.lookupRef(), explicit ? subjectEntry.copyWithExplicitEquivalents(neighbours)
                                                       : subjectEntry.copyWithDirectEquivalents(neighbours));
        
        //For each lookup, recompute its transitive closure. 
        Set<LookupEntry> newLookups = recomputeTransitiveClosures(lookups.values());

        //Write to store
        for (LookupEntry entry : newLookups) {
            entryStore.store(entry);
        }
    }

    private Iterable<LookupRef> neighbours(final Set<Publisher> publishers,
            LookupEntry subjectEntry, Set<LookupEntry> equivEntries) {
        return Iterables.concat(
                retainRefsNotInPublishers(relevantEquivalents(subjectEntry), publishers), 
                Iterables.transform(equivEntries, LookupEntry.TO_SELF)
        );
    }

    private Set<LookupRef> relevantEquivalents(LookupEntry subjectEntry) {
        return explicit ? subjectEntry.explicitEquivalents() : subjectEntry.directEquivalents();
    }

    private Iterable<LookupRef> retainRefsNotInPublishers(Set<LookupRef> directEquivalents, final Set<Publisher> publishers) {
        return Iterables.filter(directEquivalents, new Predicate<LookupRef>() {
            @Override
            public boolean apply(LookupRef input) {
                return !publishers.contains(input.publisher());
            }
        });
    }
    
    private <T extends Described> Iterable<T> filterContentPublishers(Iterable<T> content, final Set<Publisher> publishers) {
        return Iterables.filter(content, new Predicate<Described>() {
            @Override
            public boolean apply(Described input) {
                return publishers.contains(input.getPublisher());
            }
        });
    }

    private Set<LookupEntry> recomputeTransitiveClosures(Iterable<LookupEntry> lookups) {
        
        Map<String,LookupEntry> lookupMap = Maps.uniqueIndex(lookups, LookupEntry.TO_ID);
        
        Set<LookupEntry> newLookups = Sets.newHashSet();
        for (LookupEntry entry : lookups) {
            
            Set<LookupRef> transitiveSet = Sets.newHashSet();
            
            Set<LookupRef> seen = Sets.newHashSet();
            Queue<LookupRef> direct = Lists.newLinkedList(neighbours(entry));
            //Traverse equivalence graph breadth-first
            while(!direct.isEmpty()) {
                LookupRef current = direct.poll();
                if(seen.contains(current)) {
                    continue;
                } else {
                    seen.add(current);
                }
                transitiveSet.add(current);
                String id = current.id();
                direct.addAll(checkNotNull(lookupMap.get(id), "No lookup entry for " + id).directEquivalents());
                direct.addAll(lookupMap.get(id).explicitEquivalents());
            }
            
            newLookups.add(entry.copyWithEquivalents(transitiveSet));
        }
        return newLookups;
    }
    
    private Set<LookupEntry> entriesFor(Iterable<String> equivalents) {
        return ImmutableSet.copyOf(entryStore.entriesForUris(equivalents));
    }

    private LookupEntry get(String subject) {
        return Iterables.getOnlyElement(entryStore.entriesForUris(ImmutableList.of(subject)), null);
    }

    // Uses a work queue to pull out and map the transitive closures rooted at each entry in entries.
    private Map<LookupRef, LookupEntry> transitiveClosure(Set<LookupEntry> entries) {

        HashMap<LookupRef, LookupEntry> transitiveClosure = Maps.newHashMap();
        
        for (LookupEntry entry : entries) {
            transitiveClosure.put(entry.lookupRef(), entry);
            for (LookupEntry equivEntry : entriesForRefs(filter(entry.equivalents(), not(in(transitiveClosure.keySet()))))) {
                transitiveClosure.put(equivEntry.lookupRef(), equivEntry);
            }
        }
        
        return transitiveClosure;
        
    }

    private Iterable<LookupRef> neighbours(LookupEntry current) {
        return ImmutableSet.copyOf(Iterables.concat(current.directEquivalents(), current.explicitEquivalents()));
    }

    private Set<LookupEntry> entriesForRefs(Iterable<LookupRef> refs) {
        return ImmutableSet.copyOf(entryStore.entriesForUris(Iterables.transform(refs, LookupRef.TO_ID)));
    }
}
