package org.atlasapi.persistence.content.mongo;

import static org.atlasapi.persistence.content.ContentTable.CHILD_ITEMS;
import static org.atlasapi.persistence.content.ContentTable.PROGRAMME_GROUPS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_CONTAINERS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_ITEMS;

import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;

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

        for (LookupRef lookupRef : lookupRefs) {

            DBObject found = find(lookupRef);
            results.put(lookupRef.id(), toModel(found));

        }

        return results.build();
    }

    private DBObject find(LookupRef lookupRef) {
        DBCollection collection = null;

        switch (lookupRef.type()) {
        case CONTAINER:
            collection = containers;
            break;
        case PROGRAMME_GROUP:
            collection = programmeGroups;
            break;
        case TOP_LEVEL_ITEM:
            collection = topLevelItems;
            break;
        case CHILD_ITEM:
            collection = children;
            break;
        default:
            throw new IllegalArgumentException("Unknown lookup type");
        }

        return collection.findOne(lookupRef.id());
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
