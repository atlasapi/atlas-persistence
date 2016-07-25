package org.atlasapi.persistence.lookup;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.Iterables.transform;

import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.util.GroupLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.MorePredicates;
import com.metabroadcast.common.collect.MoreSets;

public class TransitiveLookupWriter implements LookupWriter {
    
    private static final GroupLock<String> lock = GroupLock.<String>natural();
    
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
    
    private static final Function<ContentRef, String> TO_URI = new Function<ContentRef, String>() {
        @Override
        public String apply(@Nullable ContentRef input) {
            return input.getCanonicalUri();
        }
    };

    @Override
    public Optional<Set<LookupEntry>> writeLookup(ContentRef subject, Iterable<ContentRef> equivalents, Set<Publisher> sources) {
        Iterable<String> neighbourUris = Iterables.transform(filterContentsources(equivalents, sources), TO_URI);
        return writeLookup(subject.getCanonicalUri(), ImmutableSet.copyOf(neighbourUris), sources);
    }
    
    private Iterable<ContentRef> filterContentsources(Iterable<ContentRef> content, final Set<Publisher> sources) {
        return Iterables.filter(content, new Predicate<ContentRef>() {
            @Override
            public boolean apply(ContentRef input) {
                return sources.contains(input.getPublisher());
            }
        });
    }
    
    public Optional<Set<LookupEntry>> writeLookup(final String subjectUri, Iterable<String> equivalentUris, final Set<Publisher> sources) {
        Preconditions.checkNotNull(emptyToNull(subjectUri), "null subject");
        
        ImmutableSet<String> newNeighboursUris = ImmutableSet.copyOf(equivalentUris);
        Set<String> subjectAndNeighbours = MoreSets.add(newNeighboursUris, subjectUri);
        Set<String> transitiveSetsUris = null;
        try {
            synchronized (lock) {
                while((transitiveSetsUris = tryLockAllIds(subjectAndNeighbours)) == null) {
                    lock.unlock(subjectAndNeighbours);
                    lock.wait();
                }
            }
            
            return updateEntries(subjectUri, newNeighboursUris, transitiveSetsUris, sources);
            
        } catch(OversizeTransitiveSetException otse) {
            log.info(String.format("Oversize set: %s + %s: %s", 
                    subjectUri, newNeighboursUris, otse.getMessage()));
            return Optional.absent();
        } catch(InterruptedException e) {
            log.error(String.format("%s: %s", subjectUri, newNeighboursUris), e);
            return Optional.absent();
        } finally {
            synchronized (lock) {
                lock.unlock(subjectAndNeighbours);
                if (transitiveSetsUris != null) {
                    lock.unlock(transitiveSetsUris);
                }
                lock.notifyAll();
            }
        }
    }

    private Optional<Set<LookupEntry>> updateEntries(String subjectUri, ImmutableSet<String> newNeighboursUris,
            Set<String> transitiveSetsUris, Set<Publisher> sources) {
        
        LookupEntry subject = entryFor(subjectUri);
        checkNotNull(subject, "No entry for %s", subjectUri);
        if(noChangeInNeighbours(subject, newNeighboursUris, sources)) {
            log.debug("{}: no change in neighbours: {}", subjectUri, newNeighboursUris);
            return Optional.absent();
        }
        
        // entries for all members in all transitive sets involved
        Map<String, LookupEntry> entryIndex = resolveTransitiveSets(transitiveSetsUris);
        
        Set<LookupEntry> newNeighbours = newSubjectNeighbours(newNeighboursUris, entryIndex);
        
        for (LookupEntry entry : entryIndex.values()) {
            entryIndex.put(entry.uri(), 
                updateEntryNeighbours(entry, subject, newNeighbours, sources)
            );
        }
   
        Set<LookupEntry> newLookups = recomputeTransitiveClosures(entryIndex);
        for (LookupEntry entry : newLookups) {
            entryStore.store(entry);
        }
        
        return Optional.of(newLookups);
    }

    private ImmutableSet<LookupEntry> newSubjectNeighbours(Set<String> neighboursUris,
            Map<String, LookupEntry> entryIndex) {
        return ImmutableSet.copyOf(Iterables.transform(neighboursUris, Functions.forMap(entryIndex)));
    }

    private Map<String, LookupEntry> resolveTransitiveSets(Set<String> transitiveSetUris) {
        return Maps.newHashMap(Maps.uniqueIndex(entriesFor(transitiveSetUris), LookupEntry.TO_ID));
    }

    /*
     * Attempts to lock the URIs of the directly affected entries before
     * resolving the entries and then attempting to lock the full equivalence
     * sets.
     * 
     * The initial lock need to be attempted since between resolving the entries
     * and locking the full equivalence sets another thread could potentially
     * have changed those entries.
     * 
     * A return value of null means either of the lock attempts failed an
     * locking needs to re-attempted. Non-null return is a set containing all
     * URIs in all transitive sets relevant to this update.
     */
    private Set<String> tryLockAllIds(Set<String> neighboursUris) throws InterruptedException {
        if (!lock.tryLock(neighboursUris)) {
            return null;
        }
        Set<LookupEntry> entries = entriesFor(neighboursUris);
        Iterable<LookupRef> transitiveSetRefs = Iterables.concat(Iterables.transform(entries, LookupEntry.TO_EQUIVS));
        Set<String> transitiveSetUris = ImmutableSet.copyOf(Iterables.transform(transitiveSetRefs, LookupRef.TO_URI));
        // We allow oversize sets if this is being written as an explicit equivalence, 
        // since a user has explicitly asked us to make the assertion, so we must
        // honour it
        if (!explicit && transitiveSetUris.size() > maxSetSize) {
            throw new OversizeTransitiveSetException(transitiveSetUris.size());
        }
        Iterable<String> urisToLock = Iterables.filter(transitiveSetUris, not(in(neighboursUris)));
        return lock.tryLock(ImmutableSet.copyOf(urisToLock)) ? transitiveSetUris
                                                             : null;
    }

    private LookupEntry updateEntryNeighbours(LookupEntry entry, LookupEntry subject,
            Set<LookupEntry> subjectNeighbours, Set<Publisher> sources) {
        if (entry.equals(subject)) {
            return updateSubjectNeighbours(subject, subjectNeighbours, sources);
        } 
        if (sources.contains(entry.lookupRef().publisher())) {
            return updateEntrysNeighbours(entry, subject, subjectNeighbours);
        }
        return entry;
    }

    private LookupEntry updateSubjectNeighbours(LookupEntry subject,
            Set<LookupEntry> neighbours, Set<Publisher> sources) {
        Predicate<LookupRef> unaffectedSources = MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, not(in(sources)));
        return updateRelevantNeighbours(subject, Iterables.concat(
            Sets.filter(getRelevantNeighbours(subject), unaffectedSources), 
            Iterables.transform(neighbours, LookupEntry.TO_SELF)
        ));
    }

    private LookupEntry updateEntrysNeighbours(LookupEntry entry,
            LookupEntry subject, Set<LookupEntry> subjectNeighbours) {
        ImmutableSet<LookupRef> subjectRef = ImmutableSet.of(subject.lookupRef());
        Set<LookupRef> entryNeighbours = getRelevantNeighbours(entry);
        if (subjectNeighbours.contains(entry)) {
            entryNeighbours = Sets.union(entryNeighbours, subjectRef);
        } else {
            entryNeighbours = Sets.difference(entryNeighbours, subjectRef);
        }
        return updateRelevantNeighbours(entry, entryNeighbours);
    }

    private LookupEntry updateRelevantNeighbours(LookupEntry equivalent,
            Iterable<LookupRef> updatedNeighbours) {
        return explicit ? equivalent.copyWithExplicitEquivalents(updatedNeighbours)
                        : equivalent.copyWithDirectEquivalents(updatedNeighbours);
    }

    private boolean noChangeInNeighbours(LookupEntry subject, ImmutableSet<String> newNeighbours,
            Set<Publisher> sources) {
        Set<LookupRef> currentNeighbours = Sets.filter(
            getRelevantNeighbours(subject), 
            MorePredicates.transformingPredicate(LookupRef.TO_SOURCE, in(sources))
        );
        Set<String> subjectAndNeighbours = MoreSets.add(newNeighbours, subject.uri());
        Set<String> currentNeighbourUris = ImmutableSet.copyOf(transform(currentNeighbours, LookupRef.TO_URI));
        boolean noChange = currentNeighbourUris.equals(subjectAndNeighbours);
        if (!noChange) {
            log.debug("Equivalence change: {} -> {}", currentNeighbourUris, subjectAndNeighbours);
        }
        return noChange;
    }

    private Set<LookupRef> getRelevantNeighbours(LookupEntry subjectEntry) {
        return explicit ? subjectEntry.explicitEquivalents() 
                        : subjectEntry.directEquivalents();
    }

    private Set<LookupEntry> recomputeTransitiveClosures(Map<String, LookupEntry> entries) {
        
        Set<LookupEntry> updatedEntries = Sets.newHashSet();
        for (LookupEntry entry : Iterables.filter(entries.values(), not(in(updatedEntries)))) {

            Set<LookupRef> transitiveSet = Sets.newHashSet();
            
            Queue<LookupRef> direct = Lists.newLinkedList(neighbours(entry));
            //Traverse equivalence graph breadth-first
            Set<LookupRef> seen = Sets.newHashSet();
            while(!direct.isEmpty()) {
                LookupRef current = direct.poll();
                if (!seen.contains(current)) {
                    transitiveSet.add(current);
                    if (entries.get(current.uri())!= null) {
                        LookupEntry currentEntry = entries.get(current.uri());
                        Iterables.addAll(direct, Iterables.filter(neighbours(currentEntry), not(in(transitiveSet))));
                    }
                    seen.add(current);
                }
            }
            
            // Because all entries in the same transitive set should have
            // the same equivalents their entries can be updated here,
            // short-circuiting the top-level loop.
            for (LookupRef lookupRef : transitiveSet) {
                LookupEntry lookupEntry = entries.get(lookupRef.uri());
                if(lookupEntry != null ) {
                    updatedEntries.add(lookupEntry.copyWithEquivalents(transitiveSet));
                }
            }
        }
        return updatedEntries;
    }

    private Iterable<LookupRef> neighbours(LookupEntry current) {
        return Iterables.concat(current.directEquivalents(), current.explicitEquivalents());
    }
    
    private Set<LookupEntry> entriesFor(Iterable<String> equivalents) {
        return ImmutableSet.copyOf(entryStore.entriesForCanonicalUris(equivalents));
    }

    private LookupEntry entryFor(String subject) {
        return Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(ImmutableList.of(subject)), null);
    }

    private static class OversizeTransitiveSetException extends RuntimeException {
        
        private int size;

        public OversizeTransitiveSetException(int size)  {
            this.size = size;
        }

        @Override
        public String getMessage() {
            return String.valueOf(size);
        }
        
    }
    
}
