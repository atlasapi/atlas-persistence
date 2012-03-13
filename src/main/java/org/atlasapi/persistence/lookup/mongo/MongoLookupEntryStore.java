package org.atlasapi.persistence.lookup.mongo;

import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.IN;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;
import static org.atlasapi.persistence.lookup.entry.LookupEntry.lookupEntryFrom;
import static org.atlasapi.persistence.lookup.entry.LookupRef.TO_ID;
import static org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator.OPAQUE_ID;

import java.util.Set;

import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.lookup.entry.LookupRef;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
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
            lookup.update(MongoBuilders.where().idEquals(idEntry.uri()).build(), translator.toDbo(idEntry), UPSERT, SINGLE);
        }
    }
    
    @Override
    public Iterable<LookupEntry> entriesForUris(Iterable<String> uris) {
        DBCursor found = lookup.find(where().idIn(uris).build());
        if (found == null) {
            return ImmutableList.of();
        }
        return Iterables.transform(found, translator.FROM_DBO);
    }

    @Override
    public Iterable<LookupEntry> entriesForIds(Iterable<Long> ids) {
        DBObject queryDbo = new BasicDBObject(OPAQUE_ID, new BasicDBObject(IN, ids));
        DBCursor found = lookup.find(queryDbo);
        if (found == null) {
            return ImmutableList.of();
        }
        return Iterables.transform(found, translator.FROM_DBO);
    }
    
    @Override
    public void ensureLookup(Content content) {
        LookupEntry newEntry = lookupEntryFrom(content);
        // Since most content will already have a lookup entry we read first to avoid locking the database
        LookupEntry existing = translator.fromDbo(lookup.findOne(new BasicDBObject(MongoConstants.ID, content.getCanonicalUri())));
        if (existing == null) {
            store(newEntry);
        } else if(!newEntry.lookupRef().category().equals(existing.lookupRef().category())) {
            convertEntry(content, newEntry, existing);
        }
    }

    private void convertEntry(Content content, LookupEntry newEntry, LookupEntry existing) {
        LookupRef ref = LookupRef.from(content);
        Set<LookupRef> directEquivs = ImmutableSet.<LookupRef>builder().add(ref).addAll(existing.directEquivalents()).build();
        Set<LookupRef> transitiveEquivs = ImmutableSet.<LookupRef>builder().add(ref).addAll(existing.equivalents()).build();
        LookupEntry merged = new LookupEntry(newEntry.uri(), existing.id(), ref, newEntry.aliases(), directEquivs, transitiveEquivs, existing.created(), newEntry.updated());
        
        store(merged);
        
        for (LookupEntry entry : entriesForUris(transform(filter(transitiveEquivs, not(equalTo(ref))), TO_ID))) {
            if(entry.directEquivalents().contains(ref)) {
                entry = entry.copyWithDirectEquivalents(ImmutableSet.<LookupRef>builder().add(ref).addAll(entry.directEquivalents()).build());
            }
            entry = entry.copyWithEquivalents(ImmutableSet.<LookupRef>builder().add(ref).addAll(existing.equivalents()).build());
            store(entry);
        }
    }

}
