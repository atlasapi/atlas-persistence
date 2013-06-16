package org.atlasapi.persistence.content.mongo;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Multimap;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoContentResolver implements KnownTypeContentResolver {

    private final ItemTranslator itemTranslator;
    private final ContainerTranslator containerTranslator;
    private final MongoContentTables contentTables;
    private final LookupEntryStore lookupEntryStore;

    public MongoContentResolver(DatabasedMongo mongo, LookupEntryStore lookupEntryStore) {
        this.contentTables = new MongoContentTables(mongo);
        SubstitutionTableNumberCodec idCodec = new SubstitutionTableNumberCodec();
        this.containerTranslator = new ContainerTranslator(idCodec);
        this.itemTranslator = new ItemTranslator(idCodec);
        this.lookupEntryStore = checkNotNull(lookupEntryStore);
    }

    public ResolvedContent findByLookupRefs(Iterable<LookupRef> lookupRefs) {
        Builder<String, Identified> results = ImmutableMap.builder();
        
        Multimap<DBCollection, String> idsGroupedByTable = HashMultimap.create();
        for (LookupRef lookupRef : lookupRefs) {
            idsGroupedByTable.put(contentTables.collectionFor(lookupRef.category()), lookupRef.uri());
        }
        
        for (Entry<DBCollection, Collection<String>> lookupInOneTable : idsGroupedByTable.asMap().entrySet()) {
            
            DBCursor found = lookupInOneTable.getKey().find(where().idIn(lookupInOneTable.getValue()).build());
            if (found != null) {
                for (DBObject dbo : found) {
                    Identified model = toModel(dbo);
                    results.put(model.getCanonicalUri(), model);
                }
            }
        }
        
        ImmutableMap<String, Identified> res = results.build();
        
        addIdsToResults(res);
        return ResolvedContent.builder().putAll(res).build();
    }

    private void addIdsToResults(ImmutableMap<String, Identified> uriToIdentified) {
        Map<String, Long> idsForCanonicalUris = lookupEntryStore.idsForCanonicalUris(uriToIdentified.keySet());
        
        for(Entry<String, Identified> result : uriToIdentified.entrySet()) {
            Long id = idsForCanonicalUris.get(result.getKey());
            result.getValue().setId(id);
        }
    }

    private Identified toModel(DBObject dbo) {
        if(dbo == null) {
            return null;
        }
        if (!dbo.containsField(DescribedTranslator.TYPE_KEY)) {
            throw new IllegalStateException("Missing type field");
        }
        EntityType type = EntityType.from((String) dbo.get(DescribedTranslator.TYPE_KEY));
        
        switch(type) {
        	case BRAND:
        	case SERIES:
        	case CONTAINER:
        		return containerTranslator.fromDBObject(dbo, null);
        	case ITEM:
        	case EPISODE:
        	case CLIP:
        	case FILM:
        	case SONG:
        		return itemTranslator.fromDBObject(dbo, null);
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }
}
