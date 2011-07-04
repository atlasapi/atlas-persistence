package org.atlasapi.persistence.lookup;

import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static org.atlasapi.media.entity.Identified.TO_URI;

import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class TransitiveLookupWriter implements LookupWriter {

    private final LookupEntryStore entryStore;

    public TransitiveLookupWriter(LookupEntryStore entryStore) {
        this.entryStore = entryStore;
    }

    @Override
    public <T extends Described> void writeLookup(final T subject, Iterable<T> directEquivalents) {
        
        Set<Described> allItems = ImmutableSet.<Described>builder().add(subject).addAll(directEquivalents).build();
        
        //canonical URIs of subject and directEquivalents
        final Set<String> canonUris = ImmutableSet.copyOf(Iterables.transform(allItems, TO_URI));

        //entry for the subject.
        LookupEntry subjectEntry = getOrCreate(subject);//.copyWithDirectEquivalents(Iterables.transform(allItems, LookupRef.FROM_DESCRIBED));
        Set<LookupEntry> equivEntries = entriesFor(directEquivalents);

        //Pull the current transitive closures for the directly equivalent parameters.
        Map<LookupRef, LookupEntry> lookups = transitiveClosure(ImmutableSet.copyOf(Iterables.concat(ImmutableSet.of(subjectEntry), equivEntries)));
        
        
        //Update the direct equivalents for all the lookups.
        lookups = Maps.newHashMap(Maps.transformValues(lookups, new Function<LookupEntry, LookupEntry>() {
            @Override
            public LookupEntry apply(LookupEntry entry) {
                if (canonUris.contains(entry.id())) {
                    return entry.copyWithDirectEquivalents(Sets.union(entry.directEquivalents(), ImmutableSet.of(LookupRef.from(subject))));
                } else {
                    return entry.copyWithDirectEquivalents(Sets.difference(entry.directEquivalents(), ImmutableSet.of(LookupRef.from(subject))));
                }
                
            }
        }));
        
        lookups.put(subjectEntry.lookupRef(), subjectEntry.copyWithDirectEquivalents(Iterables.transform(allItems, LookupRef.FROM_DESCRIBED)));
        
        //For each lookup, recompute its transitive closure. 
        Set<LookupEntry> newLookups = recomputeTransitiveClosures(lookups.values());

        //Write to store
        for (LookupEntry entry : newLookups) {
            entryStore.store(entry);
        }

    }

    private Set<LookupEntry> recomputeTransitiveClosures(Iterable<LookupEntry> lookups) {
        
        Map<String,LookupEntry> lookupMap = Maps.uniqueIndex(lookups, LookupEntry.TO_ID);
        
        Set<LookupEntry> newLookups = Sets.newHashSet();
        for (LookupEntry entry : lookups) {
            
            Set<LookupRef> transitiveSet = Sets.newHashSet();
            
            Set<LookupRef> seen = Sets.newHashSet();
            Queue<LookupRef> direct = new LinkedList<LookupRef>(entry.directEquivalents());
            //Traverse equivalence graph breadth-first
            while(!direct.isEmpty()) {
                LookupRef current = direct.poll();
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
    
    private Set<LookupEntry> entriesFor(Iterable<? extends Described> equivalents) {
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

    private Map<LookupRef, LookupEntry> transitiveClosure(Set<LookupEntry> entries) {
        
        Queue<LookupEntry> toProcess = Lists.newLinkedList(entries);
        
        Map<LookupRef, LookupEntry> found = Maps.newHashMap();
        
        while(!toProcess.isEmpty()) {
            LookupEntry current = toProcess.poll();
            found.put(current.lookupRef(), current);
            toProcess.addAll(entriesForRefs(filter(current.directEquivalents(), not(in(found.keySet())))));
        }
        
        return found;
//        Set<LookupEntry> currentEntries = Sets.newHashSet(entries);
//        Set<String> currentRoots = ImmutableSet.copyOf(lookupIds(currentEntries));
//
//        Set<String> foundUris = Sets.newHashSet(currentRoots);
//        Set<LookupEntry> found = Sets.newHashSet(currentEntries);
//
//        while (!currentRoots.isEmpty()) {
//            Set<String> stepIds = singleStep(currentEntries);
//            currentRoots = Sets.difference(stepIds, foundUris);
//
//            if (!currentRoots.isEmpty()) {
//                currentEntries = entriesForUris(currentRoots);
//                found.addAll(currentEntries);
//                foundUris.addAll(stepIds);
//            }
//        }
//
//        return found;
    }

    private Set<LookupEntry> entriesForRefs(Iterable<LookupRef> refs) {
        return ImmutableSet.copyOf(Iterables.transform(refs, new Function<LookupRef, LookupEntry>() {
            @Override
            public LookupEntry apply(LookupRef input) {
                LookupEntry entry = entryStore.entryFor(input.id());
                return entry;
            }
        }));
    }

    private Iterable<String> lookupIds(Set<LookupEntry> currentEntries) {
        return Iterables.transform(currentEntries, LookupEntry.TO_ID);
    }

    private Set<String> singleStep(Set<LookupEntry> currentEntries) {
        return ImmutableSet.copyOf(transform(concat(transform(currentEntries, LookupEntry.TO_DIRECT_EQUIVS)), LookupRef.TO_ID));
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
