package org.atlasapi.persistence.lookup;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import javax.annotation.Nullable;

import org.atlasapi.equiv.ContentRef;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.v3.ContentEquivalenceAssertionMessenger;
import org.atlasapi.messaging.v3.EquivalenceChangeMessenger;
import org.atlasapi.persistence.Transaction;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.util.GroupLock;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.mongodb.MongoCommandException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.emptyToNull;
import static org.atlasapi.persistence.lookup.entry.EquivRefs.Direction.INCOMING;

public class TransitiveLookupWriter implements LookupWriter {
    
    private static final GroupLock<String> lock = GroupLock.<String>natural();
    
    private static final Logger log = LoggerFactory.getLogger(TransitiveLookupWriter.class);
    private static final Logger timerLog = LoggerFactory.getLogger("TIMER");
    private static final int maxSetSize = 150;
    private static final int WRITE_RETRIES = 5;
    private static final Set<String> ALL_PUBLISHER_KEYS = Publisher.all().stream()
            .map(Publisher::key)
            .collect(MoreCollectors.toImmutableSet());

    private final LookupEntryStore entryStore;
    private final EquivType equivType;
    private final ContentEquivalenceAssertionMessenger equivAssertionMessenger;
    private final EquivalenceChangeMessenger equivChangeMessenger;

    public static TransitiveLookupWriter explicitTransitiveLookupWriterWithMessengers(
            LookupEntryStore entryStore,
            ContentEquivalenceAssertionMessenger equivAssertionMessenger,
            EquivalenceChangeMessenger equivChangeMessenger
    ) {
        return new TransitiveLookupWriter(entryStore, EquivType.EXPLICIT, equivAssertionMessenger, equivChangeMessenger);
    }

    public static TransitiveLookupWriter explicitTransitiveLookupWriter(LookupEntryStore entryStore) {
        return new TransitiveLookupWriter(entryStore, EquivType.EXPLICIT, null, null);
    }

    public static TransitiveLookupWriter generatedTransitiveLookupWriterWithMessengers(
            LookupEntryStore entryStore,
            ContentEquivalenceAssertionMessenger equivAssertionMessenger,
            EquivalenceChangeMessenger equivChangeMessenger
    ) {
        return new TransitiveLookupWriter(entryStore, EquivType.DIRECT, equivAssertionMessenger, equivChangeMessenger);
    }
    
    public static TransitiveLookupWriter generatedTransitiveLookupWriter(LookupEntryStore entryStore) {
        return new TransitiveLookupWriter(entryStore, EquivType.DIRECT, null, null);
    }

    public static TransitiveLookupWriter blacklistTransitiveLookupWriterWithMessengers(
            LookupEntryStore entryStore,
            ContentEquivalenceAssertionMessenger equivAssertionMessenger,
            EquivalenceChangeMessenger equivChangeMessenger
    ) {
        return new TransitiveLookupWriter(entryStore, EquivType.BLACKLIST, equivAssertionMessenger, equivChangeMessenger);
    }

    public static TransitiveLookupWriter blacklistTransitiveLookupWriter(LookupEntryStore entryStore) {
        return new TransitiveLookupWriter(entryStore, EquivType.BLACKLIST, null, null);
    }

    private TransitiveLookupWriter(
            LookupEntryStore entryStore,
            EquivType equivType,
            @Nullable ContentEquivalenceAssertionMessenger equivAssertionMessenger,
            @Nullable EquivalenceChangeMessenger equivChangeMessenger
    ) {
        this.entryStore = checkNotNull(entryStore);
        this.equivType = checkNotNull(equivType);
        this.equivAssertionMessenger = equivAssertionMessenger;
        this.equivChangeMessenger = equivChangeMessenger;
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

        for (int attempt = 1; attempt <= WRITE_RETRIES; attempt++) {

            if (attempt > 1) {
                try {
                    Thread.sleep(1000 * (long) Math.pow(2, attempt));
                } catch (InterruptedException interruptedException) {
                    throw new RuntimeException(interruptedException);
                }
            }

            try (Transaction transaction = entryStore.startTransaction()) {
                LookupEntry subjectEntry = entryFor(transaction, subjectUri);
                UpdateResult result = writeLookup(
                        transaction,
                        subjectUri,
                        subjectEntry,
                        equivalentUris,
                        sources
                );
                transaction.commit();

                Set<LookupEntry> newLookups = handleUpdateResult(result);
                return Optional.fromNullable(newLookups);
            }
            catch (MongoCommandException e) {
                // The transaction was too large due to Mongo restrictions so we have to do it without a transaction
                if (e.getErrorCode() == 257 || e.getErrorCodeName().equals("TransactionTooLarge")) {
                    log.warn("Transaction for updating {} was too large, retrying without transactions", subjectUri);
                    LookupEntry subjectEntry = entryFor(Transaction.none(), subjectUri);
                    UpdateResult result = writeLookup(Transaction.none(), subjectUri, subjectEntry, equivalentUris, sources);
                    Set<LookupEntry> newLookups = handleUpdateResult(result);
                    return Optional.fromNullable(newLookups);
                }
                else if (e.getErrorCode() == 112 || e.getErrorCodeName().equals("WriteConflict")) {
                    log.warn(
                            "WriteConflict when updating {} (attempt {}/{}), retrying",
                            new Object[] { // This is to help the compiler use the correct overloaded method
                                    subjectUri,
                                    attempt,
                                    WRITE_RETRIES
                            }
                    );
                }
                else {
                    throw e;
                }
            }
        }
        throw new IllegalStateException("Exceeded number of retry attempts to update " + subjectUri);
    }

    @Nullable
    private Set<LookupEntry> handleUpdateResult(@Nullable UpdateResult result) {
        if (result == null) {
            return null;
        }

        if (result.isSubjectOutgoingsChanged()) {
            sendEquivalenceAssertionMessage(result.getUpdatedSubject());
            sendEquivalenceChangeMessage(result.getOriginalSubject(), result.getUpdatedSubject());
        }

        return result.getAllUpdatedEntries();
    }

    @Nullable
    private UpdateResult writeLookup(
            Transaction transaction,
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
        UpdateResult subsetUpdateResult = null;
        // If we break some existing direct equivalences, update these first
        // so we can reduce the size of the transitive equiv set
        if (equivType.equals(EquivType.DIRECT)) {
            Set<String> existingSubjectDirectUris = subjectEntry.directEquivalents().getOutgoing().stream()
                    .map(LookupRef::uri)
                    .collect(MoreCollectors.toImmutableSet());
            Set<String> directUriIntersection = Sets.intersection(subjectAndNeighbours, existingSubjectDirectUris);
            strictSubset = !directUriIntersection.equals(existingSubjectDirectUris);
            boolean writeDirectSubset = strictSubset
                    && !directUriIntersection.equals(subjectAndNeighbours); //if equal we only need to update once
            if(writeDirectSubset) {
                subsetUpdateResult = writeLookup(transaction, subjectUri, subjectEntry, directUriIntersection, sources);
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
            
            UpdateResult updateResult = updateEntries(
                    transaction,
                    subjectUri,
                    newNeighboursUris,
                    transitiveSetsUris,
                    sources
            );

            return mergeResults(updateResult, subsetUpdateResult);
            
        } catch(OversizeTransitiveSetException otse) {
            log.info(String.format("Oversize set for %s : %s",
                    subjectUri, otse.getMessage()));
            return mergeResults(null, subsetUpdateResult);
        } catch(InterruptedException e) {
            log.error(String.format("%s: %s", subjectUri, newNeighboursUris), e);
            return mergeResults(null, subsetUpdateResult);
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

    @Nullable
    private UpdateResult mergeResults(@Nullable UpdateResult main, @Nullable UpdateResult other) {
        if (other == null) {
            return main;
        }
        if (main == null) {
            return other;
        }

        Map<String, LookupEntry> updatedEntries = Maps.newHashMap();

        for (LookupEntry entry : other.getAllUpdatedEntries()) {
            updatedEntries.put(entry.uri(), entry);
        }

        for (LookupEntry entry : main.getAllUpdatedEntries()) {
            updatedEntries.put(entry.uri(), entry);
        }

        return new UpdateResult(
                other.getOriginalSubject(),
                main.getUpdatedSubject(),
                ImmutableSet.copyOf(updatedEntries.values())
        );
    }

    @Nullable
    private UpdateResult updateEntries(
            Transaction transaction,
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
            return null;
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
   
        Map<String, LookupEntry> newLookups = recomputeTransitiveClosures(entryIndex);
        for (LookupEntry entry : newLookups.values()) {
            entryStore.store(transaction, entry);
        }

        timerLog.debug("TIMER L TW 4 Saved entries to db. {}ms. {}", (System.nanoTime() - lastTime) / 1000000,Thread.currentThread().getName());

        LookupEntry newSubjectLookup = newLookups.get(subject.uri());

        return new UpdateResult(
                subject,
                newSubjectLookup,
                ImmutableSet.copyOf(newLookups.values())
        );
    }

    private void sendEquivalenceAssertionMessage(LookupEntry entry) {
        if (equivAssertionMessenger == null) {
            return;
        }
        equivAssertionMessenger.sendMessage(
                entry,
                entry.getOutgoing(),
                ALL_PUBLISHER_KEYS
        );
    }

    private void sendEquivalenceChangeMessage(LookupEntry originalEntry, LookupEntry updatedEntry) {
        //No need to re-run equiv just unless direct equivs have changed
        if (equivChangeMessenger == null || !equivType.equals(EquivType.DIRECT)) {
            return;
        }
        equivChangeMessenger.sendMessageFromDirectEquivs(
                originalEntry,
                updatedEntry,
                ALL_PUBLISHER_KEYS
        );
    }

    private ImmutableSet<LookupEntry> newSubjectNeighbours(
            Set<String> neighboursUris,
            Map<String, LookupEntry> entryIndex
    ) {
        return neighboursUris.stream()
                .map(Functions.forMap(entryIndex)::apply)
                .collect(MoreCollectors.toImmutableSet());
    }

    private Map<String, LookupEntry> resolveTransitiveSets(Transaction transaction, Set<String> transitiveSetUris) {
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
            Transaction transaction,
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
        if (equivType.equals(EquivType.DIRECT) && transitiveSetUris.size() > maxSetSize && !strictSubset) {
            throw new OversizeTransitiveSetException(transitiveSetUris.size());
        }
        Set<String> urisToLock = transitiveSetUris.stream()
                .filter(uri -> !neighboursUris.contains(uri))
                .collect(MoreCollectors.toImmutableSet());

        return lock.tryLock(urisToLock) ? transitiveSetUris : null;
    }

    private LookupEntry updateEntryNeighbours(
            LookupEntry entry,
            LookupEntry subject,
            Set<LookupEntry> subjectNeighbours,
            Set<Publisher> sources
    ) {
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
        switch (equivType) {
        case EXPLICIT:
            return equivalent.copyWithExplicitEquivalents(
                    equivalent.explicitEquivalents().copyAndReplaceOutgoing(updatedNeighbours)
            );
        case DIRECT:
            return equivalent.copyWithDirectEquivalents(
                    equivalent.directEquivalents().copyAndReplaceOutgoing(updatedNeighbours)
            );
        case BLACKLIST:
            return equivalent.copyWithBlacklistedEquivalents(
                    equivalent.blacklistedEquivalents().copyAndReplaceOutgoing(updatedNeighbours)
            );
        default:
            // should never reach this, unless a new EquivType was added and its not fully implemented
            throw new IllegalStateException("Cannot determine equiv type; please verify expected writer & behaviour");
        }
    }

    private LookupEntry addIncomingNeighbour(LookupEntry equivalent, LookupRef neighbour) {
        switch (equivType) {
        case EXPLICIT:
            return equivalent.copyWithExplicitEquivalents(
                    equivalent.explicitEquivalents().copyWithLink(neighbour, INCOMING)
            );
        case DIRECT:
            return equivalent.copyWithDirectEquivalents(
                    equivalent.directEquivalents().copyWithLink(neighbour, INCOMING)
            );
        case BLACKLIST:
            return equivalent.copyWithBlacklistedEquivalents(
                    equivalent.blacklistedEquivalents().copyWithLink(neighbour, INCOMING)
            );
        default:
            // should never reach this, unless a new EquivType was added and its not fully implemented
            throw new IllegalStateException("Cannot determine equiv type; please verify expected writer & behaviour");
        }
    }

    private LookupEntry removeIncomingNeighbour(LookupEntry equivalent, LookupRef neighbour) {
        switch (equivType) {
        case EXPLICIT:
            return equivalent.copyWithExplicitEquivalents(
                    equivalent.explicitEquivalents().copyWithoutLink(neighbour, INCOMING)
            );
        case DIRECT:
            return equivalent.copyWithDirectEquivalents(
                    equivalent.directEquivalents().copyWithoutLink(neighbour, INCOMING)
            );
        case BLACKLIST:
            return equivalent.copyWithBlacklistedEquivalents(
                    equivalent.blacklistedEquivalents().copyWithoutLink(neighbour, INCOMING)
            );
        default:
            // should never reach this, unless a new EquivType was added and its not fully implemented
            throw new IllegalStateException("Cannot determine equiv type; please verify expected writer & behaviour");
        }
    }

    private boolean noChangeInNeighbours(
            LookupEntry subject,
            ImmutableSet<String> newNeighbours,
            Set<Publisher> sources
    ) {
        // we're not expecting to find ourself in the blacklisted equivs
        Set<String> subjectAndNeighbours = equivType.equals(EquivType.BLACKLIST)
                ? newNeighbours
                : Sets.union(newNeighbours, ImmutableSet.of(subject.uri()));

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
        switch (equivType) {
        case EXPLICIT:
            return subjectEntry.explicitEquivalents().getOutgoing();
        case DIRECT:
            return subjectEntry.directEquivalents().getOutgoing();
        case BLACKLIST:
            return subjectEntry.blacklistedEquivalents().getOutgoing();
        default:
            // should never reach this, unless a new EquivType was added and its not fully implemented
            throw new IllegalStateException("Cannot determine equiv type; please verify expected writer & behaviour");
        }
    }

    private Map<String, LookupEntry> recomputeTransitiveClosures(Map<String, LookupEntry> entries) {
        
        Map<String, LookupEntry> updatedEntries = Maps.newHashMap();
        for (LookupEntry entry : entries.values()) {
            if (updatedEntries.containsKey(entry.uri())) {
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
                    LookupEntry updatedEntry = lookupEntry.copyWithEquivalents(transitiveSet);
                    updatedEntries.put(updatedEntry.uri(), updatedEntry);
                }
            }
        }
        return updatedEntries;
    }
    
    private Set<LookupEntry> entriesFor(Transaction transaction, Iterable<String> equivalents) {
        return ImmutableSet.copyOf(entryStore.entriesForCanonicalUris(transaction, equivalents));
    }

    private LookupEntry entryFor(Transaction transaction, String subject) {
        return Iterables.getOnlyElement(entryStore.entriesForCanonicalUris(transaction, ImmutableSet.of(subject)), null);
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

    private static class UpdateResult {
        private final LookupEntry originalSubject;
        private final LookupEntry updatedSubject;
        private final Set<LookupEntry> allUpdatedEntries;
        private final boolean subjectOutgoingsChanged;

        public UpdateResult(
                LookupEntry originalSubject,
                LookupEntry updatedSubject,
                Set<LookupEntry> allUpdatedEntries
        ) {
            this.originalSubject = originalSubject;
            this.updatedSubject = updatedSubject;
            this.allUpdatedEntries = allUpdatedEntries;
            this.subjectOutgoingsChanged = !originalSubject.getOutgoing().equals(updatedSubject.getOutgoing());
        }

        public LookupEntry getOriginalSubject() {
            return originalSubject;
        }

        public LookupEntry getUpdatedSubject() {
            return updatedSubject;
        }

        public Set<LookupEntry> getAllUpdatedEntries() {
            return allUpdatedEntries;
        }

        public boolean isSubjectOutgoingsChanged() {
            return subjectOutgoingsChanged;
        }
    }

    private enum EquivType {
        DIRECT,
        EXPLICIT,
        BLACKLIST,
        ;
    }
    
}
