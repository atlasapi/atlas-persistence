package org.atlasapi.persistence.lookup.mongo;

import static org.atlasapi.persistence.lookup.entry.LookupEntry.lookupEntryFrom;

import org.atlasapi.media.entity.Described;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
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

        Iterable<DBObject> existing = MongoBuilders.where().idEquals(described.getCanonicalUri()).find(lookup);

        if (Iterables.isEmpty(existing)) {
            lookup.insert(translator.TO_DBO.apply(lookupEntryFrom(described)));
        }

    }
}
