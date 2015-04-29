package org.atlasapi.persistence.lookup.mongo;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.select;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.sort;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.ID;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.IN;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.SINGLE;
import static com.metabroadcast.common.persistence.mongo.MongoConstants.UPSERT;
import static org.atlasapi.media.entity.LookupRef.TO_URI;
import static org.atlasapi.persistence.lookup.entry.LookupEntry.lookupEntryFrom;
import static org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator.ACTIVELY_PUBLISHED;
import static org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator.ALIASES;
import static org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator.IDS;
import static org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator.OPAQUE_ID;
import static org.atlasapi.persistence.lookup.mongo.LookupEntryTranslator.SELF;
import static org.atlasapi.persistence.media.entity.AliasTranslator.NAMESPACE;
import static org.atlasapi.persistence.media.entity.AliasTranslator.VALUE;

import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.media.entity.IdentifiedTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.query.Selection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.ReadPreference;

public class MongoLookupEntryStore implements LookupEntryStore, NewLookupWriter {

    private static final String PUBLISHER = SELF + "." + IdentifiedTranslator.PUBLISHER;
    private static final Pattern ANYTHING = Pattern.compile("^.*");
    
    private final Logger log;
    private final DBCollection lookup;
    private final LookupEntryTranslator translator;
    private final ReadPreference readPreference;
    private final LookupEntryHasher lookupEntryHasher;
    private final PersistenceAuditLog persistenceAuditLog;

    public MongoLookupEntryStore(DBCollection lookup, PersistenceAuditLog persistenceAuditLog, 
            ReadPreference readPreference) {
        this(lookup, readPreference, persistenceAuditLog, LoggerFactory.getLogger(MongoLookupEntryStore.class));
    }

    public MongoLookupEntryStore(DBCollection lookup, ReadPreference readPreference, 
            PersistenceAuditLog persistenceAuditLog, Logger log) {
        this.lookup = checkNotNull(lookup);
        this.readPreference = checkNotNull(readPreference);
        this.persistenceAuditLog = checkNotNull(persistenceAuditLog);
        this.translator = new LookupEntryTranslator();
        this.lookupEntryHasher = new LookupEntryHasher(translator);
        this.log = checkNotNull(log);
    }
    
    @Override
    public void store(LookupEntry entry) {
        LookupEntry existing = translator.fromDbo(lookup.findOne(new BasicDBObject(MongoConstants.ID, entry.uri()), null, ReadPreference.primary()));
        store(entry, existing);
    }
    
    private void store(LookupEntry newEntry, @Nullable LookupEntry existingEntry) {
        if (existingEntry != null 
                && lookupEntryHasher.writeHashFor(newEntry) == lookupEntryHasher.writeHashFor(existingEntry)) {
            log.debug("Hash code not changed for URI {}; skipping write", newEntry.uri());
            persistenceAuditLog.logNoWrite(newEntry);
            return;
        }
        log.debug("New entry or hash code changed for URI {}; writing", newEntry.uri());
        persistenceAuditLog.logWrite(newEntry);
        lookup.update(MongoBuilders.where().idEquals(newEntry.uri()).build(), translator.toDbo(newEntry), UPSERT, SINGLE);
    }
    
    @Override
    public Iterable<LookupEntry> entriesForCanonicalUris(Iterable<? extends String> uris) {
        DBCursor found = lookup.find(where().idIn(uris).build()).setReadPreference(readPreference);
        if (found == null) {
            return ImmutableList.of();
        }
        return Iterables.transform(found, translator.FROM_DBO);
    }

    @Override
    public Iterable<LookupEntry> entriesForIds(Iterable<Long> ids) {
        DBObject queryDbo = new BasicDBObject(OPAQUE_ID, new BasicDBObject(IN, ids));
        DBCursor found = lookup.find(queryDbo).setReadPreference(readPreference);
        if (found == null) {
            return ImmutableList.of();
        }
        return Iterables.transform(found, translator.FROM_DBO);
    }
    
    @Override
    public void ensureLookup(Content content) {
        LookupEntry newEntry = lookupEntryFrom(content);
        // Since most content will already have a lookup entry we read first to avoid locking the database
        LookupEntry existing = translator.fromDbo(lookup.findOne(new BasicDBObject(MongoConstants.ID, content.getCanonicalUri()), null, ReadPreference.primary()));
        if (existing == null) {
            store(newEntry, existing);
        } else if(!newEntry.lookupRef().category().equals(existing.lookupRef().category())) {
            updateEntry(content, newEntry, existing);
        } else if (!newEntry.aliasUrls().equals(existing.aliasUrls())
                || !newEntry.aliases().equals(existing.aliases())
                || newEntry.activelyPublished() != existing.activelyPublished()) {
            store(merge(content, newEntry, existing), existing);
        } 
    }

    private void updateEntry(Content content, LookupEntry newEntry, LookupEntry existing) {
        LookupEntry merged = merge(content, newEntry, existing);
        LookupRef ref = merged.lookupRef();

        store(merged, existing);
        
        for (LookupEntry entry : entriesForCanonicalUris(transform(filter(merged.equivalents(), not(equalTo(ref))), TO_URI))) {
            if(entry.directEquivalents().contains(ref)) {
                entry = entry.copyWithDirectEquivalents(ImmutableSet.<LookupRef>builder().add(ref).addAll(entry.directEquivalents()).build());
            }
            entry = entry.copyWithEquivalents(ImmutableSet.<LookupRef>builder().add(ref).addAll(existing.equivalents()).build());
            store(entry, existing);
        }
    }

    private LookupEntry merge(Content content, LookupEntry newEntry, LookupEntry existing) {
        LookupRef ref = LookupRef.from(content);
        Set<LookupRef> directEquivs = ImmutableSet.<LookupRef>builder().add(ref).addAll(existing.directEquivalents()).build();
        Set<LookupRef> explicit = ImmutableSet.<LookupRef>builder().add(ref).addAll(existing.explicitEquivalents()).build();
        Set<LookupRef> transitiveEquivs = ImmutableSet.<LookupRef>builder().add(ref).addAll(existing.equivalents()).build();
        LookupEntry merged = new LookupEntry(newEntry.uri(), existing.id(), ref, newEntry.aliasUrls(), newEntry.aliases(), directEquivs, explicit, transitiveEquivs, existing.created(), newEntry.updated(), newEntry.activelyPublished());
        return merged;
    }

    @Override
    public Iterable<LookupEntry> entriesForIdentifiers(Iterable<? extends String> identifiers, boolean useAliases) {
        return Iterables.transform(find(identifiers), translator.FROM_DBO);
    }

    private Iterable<DBObject> find(Iterable<? extends String> identifiers) {
        return lookup.find(where().fieldIn(ALIASES, identifiers).build()).setReadPreference(readPreference);
    }

    @Override
    public Iterable<LookupEntry> entriesForAliases(Optional<String> namespace, Iterable<String> values) {
        return Iterables.transform(find(namespace, values), translator.FROM_DBO);
    }

    @Override
    public Map<String, Long> idsForCanonicalUris(Iterable<String> uris) {
        Builder<String, Long> results = ImmutableMap.builder();
        DBCursor cursor = lookup.find(
                            where().idIn(uris).build(), 
                            select().field(OPAQUE_ID).field(ID).build()
                          )
                          .setReadPreference(readPreference);
        for (DBObject dbo : cursor) {
            Long id = TranslatorUtils.toLong(dbo, OPAQUE_ID);
            if (id != null) {
                results.put(TranslatorUtils.toString(dbo, ID), id);
            }
        }
        return results.build();
    }
    
    private Iterable<DBObject> find(Optional<String> namespace, Iterable<String> values) {
        if (namespace.isPresent()) {
            return lookup.find(where().elemMatch(IDS, where().fieldEquals(NAMESPACE, namespace.get()).fieldIn(VALUE, values)).build())
                    .setReadPreference(readPreference);        
        } else {
            return lookup.find(where().elemMatch(IDS, where().fieldEquals(NAMESPACE, ANYTHING).fieldIn(VALUE, values)).build())
                    .setReadPreference(readPreference);
        }
    }

    @Override
    public Iterable<LookupEntry> entriesForPublishers(Iterable<Publisher> publishers, @Nullable Selection selection) {
    	DBCursor find = lookup.find(where()
    	                               .fieldIn(PUBLISHER, Iterables.transform(publishers, Publisher.TO_KEY))
    	                               .fieldNotEqualTo(ACTIVELY_PUBLISHED, false)
    	                               .build()
    	                           )
    	                      .setReadPreference(readPreference)
    	                      .sort(sort().ascending(OPAQUE_ID).build());
    	
    	Iterable<DBObject> result;
    	if (selection != null) {
    		find.skip(selection.getOffset());
    		result = Iterables.limit(find, selection.getLimit());
    	} else {
    	    result = find;
    	}

    	return Iterables.transform(result, translator.FROM_DBO);
    }

    public Iterable<LookupEntry> all() {
        return Iterables.transform(lookup.find(), translator.FROM_DBO);
    }
}
