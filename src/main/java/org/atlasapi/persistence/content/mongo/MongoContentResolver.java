package org.atlasapi.persistence.content.mongo;

import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.persistence.content.KnownTypeContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.DescribedTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoContentResolver implements KnownTypeContentResolver {

    private final ItemTranslator itemTranslator = new ItemTranslator();
    private final ContainerTranslator containerTranslator = new ContainerTranslator();

    private final DBCollection children;
    private final DBCollection topLevelItems;
    private final DBCollection containers;
    private final DBCollection programmeGroups;

    public MongoContentResolver(DatabasedMongo mongo) {
        this.children = mongo.collection("children");
        this.topLevelItems = mongo.collection("topLevelItems");
        this.containers = mongo.collection("containers");
        this.programmeGroups = mongo.collection("programmeGroups");
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
