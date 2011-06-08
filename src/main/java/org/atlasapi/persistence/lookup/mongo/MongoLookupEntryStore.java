package org.atlasapi.persistence.lookup.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static org.atlasapi.persistence.lookup.entry.LookupEntry.lookupEntryFrom;

import java.util.List;
import java.util.Set;

import org.atlasapi.media.entity.Described;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoLookupEntryStore implements LookupEntryStore, NewLookupWriter {

    private DBCollection lookup;
    private LookupEntryTranslator translator;

    public MongoLookupEntryStore(DatabasedMongo mongo) {
        this.lookup = mongo.collection("lookup");
        this.translator = new LookupEntryTranslator();
    }
    
    @Override
    public void store(LookupEntry entry) {
        for (LookupEntry idEntry : entry.entriesForIdentifiers()) {
            lookup.update(MongoBuilders.where().idEquals(idEntry.id()).build(), translator.toDbo(idEntry), true, false);
        }
    }
    
    @Override
    public LookupEntry entryFor(String identifier) {
        return translator.fromDbo(lookup.findOne(identifier));
    }

    @Override
    public void ensureLookup(Described described) {
        List<LookupEntry> entriesForIdentifiers = lookupEntryFrom(described).entriesForIdentifiers();
        
        Set<String> existing = getIds(MongoBuilders.where().fieldIn(MongoConstants.ID, Iterables.transform(entriesForIdentifiers, LookupEntry.TO_ID)).find(lookup));
        
        lookup.insert(Lists.transform(filterExisting(entriesForIdentifiers, existing),translator.TO_DBO));
    }

    private List<LookupEntry> filterExisting(List<LookupEntry> entriesForIdentifiers, final Set<String> existingIds) {
        
        return ImmutableList.copyOf(Iterables.filter(entriesForIdentifiers, new Predicate<LookupEntry>() {
            @Override
            public boolean apply(LookupEntry input) {
                return existingIds.contains(input.id());
            }
        }));
    }

    private Set<String> getIds(Iterable<DBObject> existing) {
        final Set<String> existingIds = ImmutableSet.copyOf(Iterables.transform(existing, new Function<DBObject, String>() {
            @Override
            public String apply(DBObject input) {
                return (String) input.get(ID);
            }
        }));
        return existingIds;
    }

}
