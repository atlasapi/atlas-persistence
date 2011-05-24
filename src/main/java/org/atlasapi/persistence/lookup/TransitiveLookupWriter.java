package org.atlasapi.persistence.lookup;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static org.atlasapi.persistence.lookup.Equivalent.TO_ID;
import static org.atlasapi.persistence.lookup.LookupEntry.TO_EQUIVS;
import static org.atlasapi.persistence.lookup.LookupEntry.lookupEntryFrom;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TransitiveLookupWriter implements LookupWriter {

    private final LookupEntryStore entryStore;

    public TransitiveLookupWriter(LookupEntryStore entryStore) {
        this.entryStore = entryStore;
    }

    @Override
    public void writeLookup(Described subject, Set<Described> directEquivalents) {
        
        //canonical URIs of subject and directEquivalents
        Set<String> canonUris = ImmutableSet.<String> builder().add(subject.getCanonicalUri()).addAll(Iterables.transform(directEquivalents, Identified.TO_URI)).build();

        //entry for the subject.
        LookupEntry subjectEntry = getOrCreate(subject).withDirectEquivalents(canonUris);

        Set<LookupEntry> equivEntries = entriesFor(directEquivalents, subject);

        //Pull the current transitive closures for the directly equivalent parameters.
        Set<LookupEntry> lookups = transitiveClosure(ImmutableSet.copyOf(Iterables.concat(ImmutableSet.of(subjectEntry), equivEntries)));
        
        Map<String,LookupEntry> lookupMap = Maps.uniqueIndex(lookups, LookupEntry.TO_ID);
        
        //Update the direct equivalents for all the lookups.
        for (LookupEntry entry : lookups) {
            if(canonUris.contains(entry.id())) {
                entry.withDirectEquivalents(Sets.union(entry.directEquivalents(), ImmutableSet.of(subject.getCanonicalUri())));
            } else {
                entry.withDirectEquivalents(Sets.difference(entry.directEquivalents(), ImmutableSet.of(subject.getCanonicalUri())));
            }
        }
        
        //For each lookup, recompute its transitive closure. 
        Set<LookupEntry> newLookups = Sets.newHashSet();
        for (LookupEntry entry : lookups) {
            
            Set<Equivalent> transitiveSet = Sets.newHashSet();
            Set<String> seen = Sets.newHashSet();
            Queue<String> direct = new LinkedList<String>(entry.directEquivalents());
            
            while(!direct.isEmpty()) {
                String next = direct.poll();
                if(seen.contains(next)) {
                    continue;
                } else {
                    seen.add(next);
                }
                LookupEntry directEntry = lookupMap.get(next);
                transitiveSet.add(directEntry.toEquivalent());
                direct.addAll(directEntry.directEquivalents());
            }
            newLookups.add(entry.copyWithEquivalents(transitiveSet));
        }

        //Write to store
        for (LookupEntry entry : newLookups) {
            entryStore.store(entry.entriesForIdentifiers());
        }

    }
    
    private Set<LookupEntry> entriesFor(Set<Described> equivalents, final Described subject) {
        return ImmutableSet.copyOf(Iterables.transform(equivalents, new Function<Described, LookupEntry>() {
            @Override
            public LookupEntry apply(Described input) {
                LookupEntry entry = entryStore.entryFor(input.getCanonicalUri());
                return entry != null ? entry : getOrCreate(input);
            }
        }));
    }

    private LookupEntry getOrCreate(Described subject) {
        LookupEntry subjectEntry = entryStore.entryFor(subject.getCanonicalUri());
        return subjectEntry != null ? subjectEntry : lookupEntryFrom(subject);
    }

    private Set<LookupEntry> transitiveClosure(Set<LookupEntry> entries) {
        Set<LookupEntry> currentEntries = Sets.newHashSet(entries);
        Set<String> currentRoots = ImmutableSet.copyOf(lookupIds(currentEntries));

        Set<String> foundUris = Sets.newHashSet(currentRoots);
        Set<LookupEntry> found = Sets.newHashSet(currentEntries);

        while (!currentRoots.isEmpty()) {
            Set<String> stepIds = singleStep(currentEntries);
            currentRoots = Sets.difference(stepIds, foundUris);

            if (!currentRoots.isEmpty()) {
                currentEntries = entriesForUris(currentRoots);
                found.addAll(currentEntries);
                foundUris.addAll(stepIds);
            }
        }

        return found;
    }

    private Iterable<String> lookupIds(Set<LookupEntry> currentEntries) {
        return Iterables.transform(currentEntries, LookupEntry.TO_ID);
    }

    private Set<String> singleStep(Set<LookupEntry> currentEntries) {
        return ImmutableSet.copyOf(transform(concat(transform(currentEntries, TO_EQUIVS)), TO_ID));
    }

    private Set<LookupEntry> entriesForUris(Set<String> roots) {
        return ImmutableSet.copyOf(Iterables.transform(roots, new Function<String, LookupEntry>() {
            @Override
            public LookupEntry apply(String input) {
                return entryStore.entryFor(input);
            }
        }));
    }

}
