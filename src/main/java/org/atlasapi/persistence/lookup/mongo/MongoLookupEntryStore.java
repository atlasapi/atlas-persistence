package org.atlasapi.persistence.lookup.mongo;

import static org.atlasapi.persistence.lookup.entry.LookupEntry.lookupEntryFrom;

import org.atlasapi.media.entity.Described;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

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
    public Iterable<LookupEntry> entriesFor(Iterable<String> ids) {
        DBCursor found = lookup.find(where().idIn(ids).build());
        if (found == null) {
            return ImmutableList.of();
        }
        return Iterables.transform(found, translator.FROM_DBO);
    }

    @Override
    public void ensureLookup(Described described) {
        // Since most content will already have a lookup entry we read first to avoid locking the database
        if (rowMissing(described)) {
            lookup.insert(translator.TO_DBO.apply(lookupEntryFrom(described)));
        }
    }

    private boolean rowMissing(Described described) {
        DBObject existing = lookup.findOne(new BasicDBObject(MongoConstants.ID, described.getCanonicalUri()), new BasicDBObject(MongoConstants.ID, 1));
        return existing == null;
    }
}
