package org.atlasapi.persistence.content.mongo;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.LookupRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ResolvedContent.ResolvedContentBuilder;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
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

    public MongoContentResolver(DatabasedMongo mongo) {

        this.children = mongo.collection("children");
        this.topLevelItems = mongo.collection("topLevelItems");
        this.containers = mongo.collection("containers");
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
        if (!dbo.containsField("type")) {
            throw new IllegalStateException("Missing type field");
        }
        String type = (String) dbo.get("type");
        if (Brand.class.getSimpleName().equals(type)) {
            return containerTranslator.fromDBObject(dbo, null);
        }
        if (Series.class.getSimpleName().equals(type)) {
            return containerTranslator.fromDBObject(dbo, null);
        }
        if (Container.class.getSimpleName().equals(type)) {
            return containerTranslator.fromDBObject(dbo, null);
        }
        if (Episode.class.getSimpleName().equals(type)) {
            return itemTranslator.fromDBObject(dbo, null);
        }
        if (Clip.class.getSimpleName().equals(type)) {
            return itemTranslator.fromDBObject(dbo, null);
        }
        if (Item.class.getSimpleName().equals(type)) {
            return itemTranslator.fromDBObject(dbo, null);
        }
        if (Film.class.getSimpleName().equals(type)) {
            return itemTranslator.fromDBObject(dbo, null);
        }
        throw new IllegalArgumentException("Unknown type: " + type);
    }
}
