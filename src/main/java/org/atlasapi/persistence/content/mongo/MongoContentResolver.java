package org.atlasapi.persistence.content.mongo;

import static org.atlasapi.persistence.content.ContentTable.CHILD_ITEMS;
import static org.atlasapi.persistence.content.ContentTable.PROGRAMME_GROUPS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_CONTAINERS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_ITEMS;

import java.util.Collection;
import java.util.Map.Entry;

import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.*;

public class MongoContentResolver implements KnownTypeContentResolver {

    private final ItemTranslator itemTranslator = new ItemTranslator();
    private final ContainerTranslator containerTranslator = new ContainerTranslator();

    private final DBCollection children;
    private final DBCollection topLevelItems;
    private final DBCollection containers;
    private final DBCollection programmeGroups;

    public MongoContentResolver(MongoContentTables contentTables) {
        children = contentTables.collectionFor(CHILD_ITEMS);
        topLevelItems = contentTables.collectionFor(TOP_LEVEL_ITEMS);
        containers = contentTables.collectionFor(TOP_LEVEL_CONTAINERS);
        programmeGroups = contentTables.collectionFor(PROGRAMME_GROUPS);
    }

    public ResolvedContent findByLookupRefs(Iterable<LookupRef> lookupRefs) {
        ResolvedContentBuilder results = ResolvedContent.builder();

        Multimap<DBCollection, String> idsGroupedByTable = HashMultimap.create();
        for (LookupRef lookupRef : lookupRefs) {
            idsGroupedByTable.put(collectionFor(lookupRef), lookupRef.id());
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
        return results.build();
    }
    
    private DBCollection collectionFor(LookupRef lookupRef) {
        switch (lookupRef.type()) {
            case CONTAINER:
                return containers;
            case PROGRAMME_GROUP:
                return programmeGroups;
            case TOP_LEVEL_ITEM:
                return topLevelItems;
            case CHILD_ITEM:
                return children;
            default:
                throw new IllegalArgumentException("Unknown lookup type");
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
        		return itemTranslator.fromDBObject(dbo, null);
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }
}
