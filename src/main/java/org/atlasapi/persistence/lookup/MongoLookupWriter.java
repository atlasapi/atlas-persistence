package org.atlasapi.persistence.lookup;

import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static org.atlasapi.persistence.lookup.Equivalent.TO_ID;
import static org.atlasapi.persistence.lookup.LookupEntry.TO_EQUIVS;

import java.util.Set;

import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoLookupWriter implements LookupWriter {
    
    private DBCollection lookup;
    private LookupEntryTranslator translator;

    public MongoLookupWriter(DatabasedMongo db) {
        this.lookup = db.collection("lookup");
        this.translator = new LookupEntryTranslator();
    }

    @Override
    public void writeLookup(Described subject, Set<Described> equivalents) {
        
        LookupEntry subjectEntry = getOrCreate(subject);
        
        Set<LookupEntry> equivEntries = entriesFor(equivalents);

        Set<LookupEntry> lookups = transitiveClosure(ImmutableSet.copyOf(Iterables.concat(ImmutableSet.of(subjectEntry), equivEntries)));
        
        
        Set<Equivalent> allEquivs = ImmutableSet.copyOf(Iterables.concat(Iterables.transform(lookups, LookupEntry.TO_EQUIVS)));
        if(equivalents.size()+1 < subjectEntry.equivalents().size()) {
            Set<String> canonUris = ImmutableSet.<String>builder().add(subject.getCanonicalUri()).addAll(Iterables.transform(equivalents, Identified.TO_URI)).build();
            for (LookupEntry entry : lookups) {
                Iterable<Equivalent> entryEquivs;
                if(canonUris.contains(entry.id())) {
                    entryEquivs = Iterables.filter(allEquivs, equivUriIn(canonUris));
                } else {
                    entryEquivs = Iterables.filter(allEquivs, Predicates.not(equivUriIn(canonUris)));
                }
                lookup.update(where().fieldIn(ID, entry.identifiers()).build(), updateFrom(translator.toDbo(entry.copyWithEquivalents(entryEquivs))), true, true);
            }
        } else {
            for (LookupEntry entry : lookups) {
                lookup.update(where().fieldIn(ID, entry.identifiers()).build(), updateFrom(translator.toDbo(entry.copyWithEquivalents(allEquivs))), true, true);
            }
        }
        
    }

    private Predicate<Equivalent> equivUriIn(final Set<String> canonUris) {
        return new Predicate<Equivalent>() {
            @Override
            public boolean apply(Equivalent input) {
                return canonUris.contains(input.id());
            }
        };
    }

    private Set<LookupEntry> entriesFor(Set<Described> equivalents) {
        return ImmutableSet.copyOf(Iterables.transform(equivalents, new Function<Described, LookupEntry>() {
            @Override
            public LookupEntry apply(Described input) {
                LookupEntry entry = lookup(input.getCanonicalUri());
                return entry != null ? entry : getOrCreate(input);
            }
        }));
    }

    private LookupEntry getOrCreate(Described subject) {
        LookupEntry subjectEntry = lookup(subject.getCanonicalUri());
        if (subjectEntry != null) {
            return subjectEntry;
        }
        subjectEntry = LookupEntry.lookupEntryFrom(subject);
        lookup.insert(Lists.transform(subjectEntry.entriesForIdentifiers(), new Function<LookupEntry, DBObject>() {
            @Override
            public DBObject apply(LookupEntry input) {
                return translator.toDbo(input);
            }
        }));
        return subjectEntry;
    }

    private DBObject updateFrom(DBObject dbo) {
        dbo.removeField(ID);
        return new BasicDBObject(MongoConstants.SET, dbo);
    }

    private LookupEntry lookup(String uri) {
        return translator.fromDbo(lookup.findOne(uri));
    }

    private Set<LookupEntry> transitiveClosure(Set<LookupEntry> entries) {
        Set<LookupEntry> currentEntries = Sets.newHashSet(entries);
        Set<String> currentRoots = ImmutableSet.copyOf(lookupIds(currentEntries));
        
        Set<String> foundUris = Sets.newHashSet(currentRoots);
        Set<LookupEntry> found = Sets.newHashSet(currentEntries);
        
        while(!currentRoots.isEmpty()) {
            Set<String> stepIds = singleStep(currentEntries);
            currentRoots = Sets.difference(stepIds, foundUris);
            
            if(!currentRoots.isEmpty()) {
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
        return ImmutableSet.copyOf(Iterables.transform(lookup.find(where().fieldIn(ID, roots).build()), new Function<DBObject, LookupEntry>() {
            @Override
            public LookupEntry apply(DBObject input) {
                return translator.fromDbo(input);
            }
        }));
    }

}
