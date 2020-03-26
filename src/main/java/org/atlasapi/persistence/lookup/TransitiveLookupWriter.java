package org.atlasapi.persistence.lookup;

import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metabroadcast.common.stream.MoreCollectors;
import com.mongodb.MongoCommandException;
import org.atlasapi.equiv.ContentRef;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.Transaction;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.util.GroupLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.emptyToNull;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.Direction.INCOMING;

public class TransitiveLookupWriter implements LookupWriter {
    
    private static final GroupLock<String> lock = GroupLock.<String>natural();
    
    private static final Logger log = LoggerFactory.getLogger(TransitiveLookupWriter.class);
    private static final Logger timerLog = LoggerFactory.getLogger("TIMER");
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
    public Optional<Set<LookupEntry>> writeLookup(ContentRef subject, Iterable<ContentRef> equivalents, Set<Publisher> sources) {

        long startTime = System.nanoTime();
        timerLog.debug("TIMER L TW 1 entered. {}", Thread.currentThread().getName());

        Iterable<String> neighbourUris = Iterables.transform(
                filterContentSources(equivalents, sources),
                ContentRef::getCanonicalUri
        );
        Optional<Set<LookupEntry>> setOptional = writeLookup(
                subject.getCanonicalUri(),
                ImmutableSet.copyOf(neighbourUris),
                sources
        );
        timerLog.debug("TIMER L TW 1 wrote lookup {}ms. {}", (System.nanoTime() - startTime) / 1000000, Thread.currentThread().getName());
        if((System.nanoTime() - startTime)/1000000 > 1000){
            timerLog.debug("TIMER L TW SLOW");
        }
        return setOptional;
    }
    
    private Iterable<ContentRef> filterContentSources(Iterable<ContentRef> content, final Set<Publisher> sources) {
        return Iterables.filter(content, input -> sources.contains(input.getPublisher()));
    }

    public Optional<Set<LookupEntry>> writeLookup(final String subjectUri, Iterable<String> equivalentUris, final Set<Publisher> sources) {
        try (@Nullable Transaction transaction = entryStore.startTransaction()) {
            LookupEntry subjectEntry = entryFor(transaction, subjectUri);
            return writeLookup(transaction, subjectUri, subjectEntry, equivalentUris, sources);
        } catch (MongoCommandException e) {
            // The transaction was too large due to Mongo restrictions so we have to do it without a transaction
            if (e.getErrorCode() == 257 || e.getErrorCodeName().equals("TransactionTooLarge")) {
                LookupEntry subjectEntry = entryFor(null, subjectUri);
                return writeLookup(null, subjectUri, subjectEntry, equivalentUris, sources);
            }
            throw e;
        }
    }

    public Optional<Set<LookupEntry>> writeLookup(
            @Nullable Transaction transaction,
            final String subjectUri,
            LookupEntry subjectEntry,
            Iterable<String> equivalentUris,
            final Set<Publisher> sources
    ) {

        long startTime = System.nanoTime();
        long lastTime = System.nanoTime();
        timerLog.debug("TIMER L TW 2 doing write lookup. {}", Thread.currentThread().getName());
        Preconditions.checkNotNull(emptyToNull(subjectUri), "null subject");
        
        ImmutableSet<String> newNeighboursUris = ImmutableSet.copyOf(equivalentUris);
        Set<String> subjectAndNeighbours = Sets.union(newNeighboursUris, ImmutableSet.of(subjectUri));
        Set<String> transitiveSetsUris = null;

        boolean strictSubset = false;
        // If we break some existing direct equivalences, update these first
        // so we can reduce the size of the transitive equiv set
        if (!explicit) {
            Set<String> existingSubjectDirectUris = subjectEntry.directEquivalents().getOutgoing().stream()
                    .map(LookupRef::uri)
                    .collect(MoreCollectors.toImmutableSet());
            Set<String> directUriIntersection = Sets.intersection(subjectAndNeighbours, existingSubjectDirectUris);
            strictSubset = !directUriIntersection.equals(existingSubjectDirectUris);
            boolean writeDirectSubset = strictSubset
                    && !directUriIntersection.equals(subjectAndNeighbours); //if equal we only need to update once
            if(writeDirectSubset) {
                writeLookup(transaction, subjectUri, subjectEntry, directUriIntersection, sources);
                strictSubset = false; //for the entire set of neighbours
            }
            //Carry on with the entire set of neighbours
        }

        try {
            synchronized (lock) {
                timerLog.debug("TIMER L TW 2 acquired the lock object. {}ms. {}", (System.nanoTime() - lastTime) / 1000000, Thread.currentThread().getName());
                lastTime = System.nanoTime();
                int loop = 0;
                while((transitiveSetsUris = tryLockAllIds(transaction, subjectAndNeighbours, strictSubset)) == null) {
                    timerLog.debug("TIMER L TW 2 failed to lock ids (loop "+loop++ +"). {}ms. {}", (System.nanoTime() - lastTime) / 1000000, Thread.currentThread().getName());
                    lastTime = System.nanoTime();
                    lock.unlock(subjectAndNeighbours);
                    timerLog.debug("TIMER L TW 2 unlocked "+subjectAndNeighbours.size()+" (loop "+loop++ +"). {}ms. {}", (System.nanoTime() - lastTime) / 1000000, Thread.currentThread().getName());
                    lastTime = System.nanoTime();
                    lock.wait();
                    timerLog.debug("TIMER L TW 2 wait finished (loop "+loop++ +"). {}ms. {}", (System.nanoTime() - lastTime) / 1000000, Thread.currentThread().getName());
                    lastTime = System.nanoTime();
                }
                timerLog.debug("TIMER L TW 2 all ids locked (loop "+loop +"). {}ms. {}", (System.nanoTime() - lastTime) / 1000000, Thread.currentThread().getName());
                lastTime = System.nanoTime();
            }
            
            return updateEntries(transaction, subjectUri, newNeighboursUris, transitiveSetsUris, sources);
            
        } catch(OversizeTransitiveSetException otse) {
            log.info(String.format("Oversize set for %s : %s",
                    subjectUri, otse.getMessage()));
            return Optional.absent();
        } catch(InterruptedException e) {
            log.error(String.format("%s: %s", subjectUri, newNeighboursUris), e);
            return Optional.absent();
        } finally {
            timerLog.debug("TIMER L TW Finally block reached. {}ms. {}", (System.nanoTime() - lastTime) / 1000000, Thread.currentThread().getName());
            lastTime = System.nanoTime();
            synchronized (lock) {
                timerLog.debug("TIMER L TW Finally acquired the lock object.{}ms. {}", (System.nanoTime() - lastTime) / 1000000, Thread.currentThread().getName());
                lastTime = System.nanoTime();
                lock.unlock(subjectAndNeighbours);
                timerLog.debug("TIMER L TW Finally unlocked subjet and neighbours ("+subjectAndNeighbours.size()+").{}ms. {}", (System.nanoTime() - lastTime) / 1000000, Thread.currentThread().getName());
                lastTime = System.nanoTime();
                if (transitiveSetsUris != null) {
                    lock.unlock(transitiveSetsUris);
                    timerLog.debug("TIMER L TW Finally unlocked transitive uris ("+transitiveSetsUris.size()+").{}ms. {}", (System.nanoTime() - lastTime) / 1000000, Thread.currentThread().getName());
                }
                lock.notifyAll();
            }
        }
    }

    private Optional<Set<LookupEntry>> updateEntries(
            @Nullable Transaction transaction,
            String subjectUri,
            ImmutableSet<String> newNeighboursUris,
            Set<String> transitiveSetsUris,
            Set<Publisher> sources
    ) {
        long startTime = System.nanoTime();
        long lastTime = System.nanoTime();
        timerLog.debug("TIMER L TW 4 updating all entries {}", Thread.currentThread().getName());
        LookupEntry subject = entryFor(transaction, subjectUri);

        timerLog.debug("TIMER L TW 4 got lookup for main entry. {}ms. {}", (System.nanoTime() - lastTime) / 1000000, Thread.currentThread().getName());
        lastTime = System.nanoTime();

        checkNotNull(subject, "No entry for %s", subjectUri);
        if(noChangeInNeighbours(subject, newNeighboursUris, sources)) {
            log.debug("{}: no change in neighbours: {}", subjectUri, newNeighboursUris);
            return Optional.absent();
        }
        
        // entries for all members in all transitive sets involved
        Map<String, LookupEntry> entryIndex = resolveTransitiveSets(transaction, transitiveSetsUris);

        timerLog.debug("TIMER L TW 4 Resolved transitive sets ("+entryIndex.size()+"). {}ms. {}", (System.nanoTime() - lastTime) / 1000000, Thread.currentThread().getName());
        lastTime = System.nanoTime();
        Set<LookupEntry> newNeighbours = newSubjectNeighbours(newNeighboursUris, entryIndex);
        
        for (LookupEntry entry : entryIndex.values()) {
            entryIndex.put(entry.uri(), 
                updateEntryNeighbours(entry, subject, newNeighbours, sources)
            );
        }
   
        Set<LookupEntry> newLookups = recomputeTransitiveClosures(entryIndex);
        for (LookupEntry entry : newLookups) {
            entryStore.store(transaction, entry);
        }

        timerLog.debug("TIMER L TW 4 Saved entries to db. {}ms. {}", (System.nanoTime() - lastTime) / 1000000,Thread.currentThread().getName());
        
        return Optional.of(newLookups);
    }

    private ImmutableSet<LookupEntry> newSubjectNeighbours(
            Set<String> neighboursUris,
            Map<String, LookupEntry> entryIndex
    ) {
        return neighboursUris.stream()
                .map(Functions.forMap(entryIndex)::apply)
                .collect(MoreCollectors.toImmutableSet());
    }

    private Map<String, LookupEntry> resolveTransitiveSets(@Nullable Transaction transaction, Set<String> transitiveSetUris) {
        return Maps.newHashMap(Maps.uniqueIndex(entriesFor(transaction, transitiveSetUris), LookupEntry::uri));
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
    @Nullable
    private Set<String> tryLockAllIds(
            @Nullable Transaction transaction,
            Set<String> neighboursUris,
            boolean strictSubset
    ) throws InterruptedException {
        long startTime = System.nanoTime();
        long lastTime = System.nanoTime();
        timerLog.debug("TIMER L TW 3 Trying to lock all ids. {}", Thread.currentThread().getName());
        if (!lock.tryLock(neighboursUris)) {
            timerLog.debug("TIMER L TW 3 Failed. {}ms. {}", (System.nanoTime() - lastTime) / 1000000, Thread.currentThread().getName());
            lastTime = System.nanoTime();
            return null;
        }

        timerLog.debug("TIMER L TW 3 all ids locked. {}ms. {}", (System.nanoTime() - lastTime) / 1000000,  Thread.currentThread().getName());
        lastTime = System.nanoTime();
        Set<LookupEntry> entries = entriesFor(transaction, neighboursUris);

        timerLog.debug("TIMER L TW 3 got all entries from the DB ("+entries.size()+"). {}ms. {}", (System.nanoTime() - lastTime) / 1000000, Thread.currentThread().getName());

        Set<String> transitiveSetUris = entries.stream()
                .map(LookupEntry::equivalents)
                .flatMap(Collection::stream)
                .map(LookupRef::uri)
                .collect(MoreCollectors.toImmutableSet());

        // We allow oversize sets if this is being written as an explicit equivalence, 
        // since a user has explicitly asked us to make the assertion, so we must
        // honour it
        // If we will shrink the direct equivalences then we allow this as well
        if (!explicit && transitiveSetUris.size() > maxSetSize && !strictSubset) {
            throw new OversizeTransitiveSetException(transitiveSetUris.size());
        }
        Set<String> urisToLock = transitiveSetUris.stream()
                .filter(uri -> !neighboursUris.contains(uri))
                .collect(MoreCollectors.toImmutableSet());

        return lock.tryLock(urisToLock) ? transitiveSetUris : null;
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

    private LookupEntry updateSubjectNeighbours(
            LookupEntry subject,
            Set<LookupEntry> neighbours,
            Set<Publisher> sources
    ) {
        Set<LookupRef> relevantNeighbours = Sets.union(
                neighbours.stream()
                        .map(LookupEntry::lookupRef)
                        .collect(MoreCollectors.toImmutableSet()),
                getRelevantOutgoingNeighbours(subject).stream()
                        .filter(neighbour -> !sources.contains(neighbour.publisher()))
                        .collect(MoreCollectors.toImmutableSet())
        );

        return updateOutgoingNeighbours(subject, relevantNeighbours);
    }

    private LookupEntry updateEntrysNeighbours(
            LookupEntry entry,
            LookupEntry subject,
            Set<LookupEntry> subjectNeighbours
    ) {
        if (subjectNeighbours.contains(entry)) {
            return addIncomingNeighbour(entry, subject.lookupRef());
        } else {
            return removeIncomingNeighbour(entry, subject.lookupRef());
        }
    }

    private LookupEntry updateOutgoingNeighbours(LookupEntry equivalent, Set<LookupRef> updatedNeighbours) {
        if (explicit) {
            return equivalent.copyWithExplicitEquivalents(
                    equivalent.explicitEquivalents().copyAndReplaceOutgoing(updatedNeighbours)
            );
        } else {
            return equivalent.copyWithDirectEquivalents(
                    equivalent.directEquivalents().copyAndReplaceOutgoing(updatedNeighbours)
            );
        }
    }

    private LookupEntry addIncomingNeighbour(LookupEntry equivalent, LookupRef neighbour) {
        if (explicit) {
            return equivalent.copyWithExplicitEquivalents(
                    equivalent.explicitEquivalents().copyWithLink(neighbour, INCOMING)
            );
        } else {
            return equivalent.copyWithDirectEquivalents(
                    equivalent.directEquivalents().copyWithLink(neighbour, INCOMING)
            );
        }
    }

    private LookupEntry removeIncomingNeighbour(LookupEntry equivalent, LookupRef neighbour) {
        if (explicit) {
            return equivalent.copyWithExplicitEquivalents(
                    equivalent.explicitEquivalents().copyWithoutLink(neighbour, INCOMING)
            );
        } else {
            return equivalent.copyWithDirectEquivalents(
                    equivalent.directEquivalents().copyWithoutLink(neighbour, INCOMING)
            );
        }
    }

    private boolean noChangeInNeighbours(
            LookupEntry subject,
            ImmutableSet<String> newNeighbours,
            Set<Publisher> sources
    ) {
        Set<String> subjectAndNeighbours = Sets.union(newNeighbours, ImmutableSet.of(subject.uri()));
        Set<String> currentNeighbourUris = getRelevantOutgoingNeighbours(subject).stream()
                .filter(neighbour -> sources.contains(neighbour.publisher()))
                .map(LookupRef::uri)
                .collect(MoreCollectors.toImmutableSet());

        boolean noChange = currentNeighbourUris.equals(subjectAndNeighbours);
        if (!noChange) {
            log.debug("Equivalence change: {} -> {}", currentNeighbourUris, subjectAndNeighbours);
        }
        return noChange;
    }

    private Set<LookupRef> getRelevantOutgoingNeighbours(LookupEntry subjectEntry) {
        return explicit ? subjectEntry.explicitEquivalents().getOutgoing()
                        : subjectEntry.directEquivalents().getOutgoing();
    }

    private Set<LookupEntry> recomputeTransitiveClosures(Map<String, LookupEntry> entries) {
        
        Set<LookupEntry> updatedEntries = Sets.newHashSet();
        for (LookupEntry entry : entries.values()) {
            if (updatedEntries.contains(entry)) {
                continue;
            }

            Set<LookupRef> transitiveSet = Sets.newHashSet();
            
            Queue<LookupRef> neighboursToProcess = Lists.newLinkedList(entry.getNeighbours());
            //Traverse equivalence graph breadth-first
            Set<LookupRef> seen = Sets.newHashSet();
            while (!neighboursToProcess.isEmpty()) {
                LookupRef current = neighboursToProcess.poll();
                if (!seen.contains(current)) {
                    transitiveSet.add(current);
                    if (entries.get(current.uri()) != null) {
                        LookupEntry currentEntry = entries.get(current.uri());
                        currentEntry.getNeighbours().stream()
                                .filter(neighbour -> !transitiveSet.contains(neighbour))
                                .forEach(neighboursToProcess::add);
                    }
                    seen.add(current);
                }
            }

            // Because all entries in the same transitive set should have
            // the same equivalents their entries can be updated here,
            // short-circuiting the top-level loop.
            for (LookupRef lookupRef : transitiveSet) {
                LookupEntry lookupEntry = entries.get(lookupRef.uri());
                if (lookupEntry != null) {
                    updatedEntries.add(lookupEntry.copyWithEquivalents(transitiveSet));
                }
            }
        }
        return updatedEntries;
    }
    
    private Set<LookupEntry> entriesFor(@Nullable Transaction transaction, Iterable<String> equivalents) {
        return ImmutableSet.copyOf(entryStore.entriesForCanonicalUris(transaction, equivalents));
    }

    private LookupEntry entryFor(@Nullable Transaction transaction, String subject) {
        return Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(transaction, ImmutableList.of(subject)), null);
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
