package org.atlasapi.persistence.lookup;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static org.atlasapi.media.entity.Identified.TO_URI;
import static org.atlasapi.persistence.lookup.entry.Equivalent.TO_ID;
import static org.atlasapi.persistence.lookup.entry.LookupEntry.TO_EQUIVS;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.atlasapi.media.entity.Described;
import org.atlasapi.persistence.lookup.entry.Equivalent;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

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
    public void writeLookup(final Described subject, Set<Described> directEquivalents) {
        
        Set<Described> allItems = ImmutableSet.<Described>builder().add(subject).addAll(directEquivalents).build();
        
        //canonical URIs of subject and directEquivalents
        final Set<String> canonUris = ImmutableSet.copyOf(Iterables.transform(allItems, TO_URI));

        //entry for the subject.
        LookupEntry subjectEntry = getOrCreate(subject).copyWithDirectEquivalents(Iterables.transform(allItems, Equivalent.FROM_DESCRIBED));
        Set<LookupEntry> equivEntries = entriesFor(directEquivalents);

        //Pull the current transitive closures for the directly equivalent parameters.
        Iterable<LookupEntry> lookups = transitiveClosure(ImmutableSet.copyOf(Iterables.concat(ImmutableSet.of(subjectEntry), equivEntries)));
        
        //Update the direct equivalents for all the lookups.
        lookups = ImmutableSet.copyOf(Iterables.transform(lookups, new Function<LookupEntry, LookupEntry>() {
            @Override
            public LookupEntry apply(LookupEntry entry) {
                if (canonUris.contains(entry.id())) {
                    return entry.copyWithDirectEquivalents(Sets.union(entry.directEquivalents(), ImmutableSet.of(Equivalent.from(subject))));
                } else {
                    return entry.copyWithDirectEquivalents(Sets.difference(entry.directEquivalents(), ImmutableSet.of(Equivalent.from(subject))));
                }
            }
        }));
        
        //For each lookup, recompute its transitive closure. 
        Set<LookupEntry> newLookups = recomputeTransitiveClosures(lookups);

        //Write to store
        for (LookupEntry entry : newLookups) {
            entryStore.store(entry);
        }

    }

    private Set<LookupEntry> recomputeTransitiveClosures(Iterable<LookupEntry> lookups) {
        
        Map<String,LookupEntry> lookupMap = Maps.uniqueIndex(lookups, LookupEntry.TO_ID);
        
        Set<LookupEntry> newLookups = Sets.newHashSet();
        for (LookupEntry entry : lookups) {
            
            Set<Equivalent> transitiveSet = Sets.newHashSet();
            
            Set<Equivalent> seen = Sets.newHashSet();
            Queue<Equivalent> direct = new LinkedList<Equivalent>(entry.directEquivalents());
            //Traverse equivalence graph breadth-first
            while(!direct.isEmpty()) {
                Equivalent current = direct.poll();
                if(seen.contains(current)) {
                    continue;
                } else {
                    seen.add(current);
                }
                transitiveSet.add(current);
                direct.addAll(lookupMap.get(current.id()).directEquivalents());
            }
            
            newLookups.add(entry.copyWithEquivalents(transitiveSet));
        }
        return newLookups;
    }
    
    private Set<LookupEntry> entriesFor(Set<Described> equivalents) {
        return ImmutableSet.copyOf(Iterables.transform(equivalents, new Function<Described, LookupEntry>() {
            @Override
            public LookupEntry apply(Described input) {
                return getOrCreate(input);
            }
        }));
    }

    private LookupEntry getOrCreate(Described subject) {
        LookupEntry subjectEntry = entryStore.entryFor(subject.getCanonicalUri());
        return subjectEntry != null ? subjectEntry : LookupEntry.lookupEntryFrom(subject);
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
