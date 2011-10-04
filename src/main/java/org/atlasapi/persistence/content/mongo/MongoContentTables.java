package org.atlasapi.persistence.content.mongo;

import static org.atlasapi.persistence.content.ContentCategory.CHILD_ITEM;
import static org.atlasapi.persistence.content.ContentCategory.CONTAINER;
import static org.atlasapi.persistence.content.ContentCategory.PROGRAMME_GROUP;
import static org.atlasapi.persistence.content.ContentCategory.TOP_LEVEL_ITEM;

import org.atlasapi.persistence.content.ContentCategory;

import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;

public class MongoContentTables {

    private ImmutableMap<ContentCategory, DBCollection> tableMap;

    public MongoContentTables(DatabasedMongo mongo) {
        this.tableMap = ImmutableMap.of(
                CONTAINER, mongo.collection(CONTAINER.tableName()),
                TOP_LEVEL_ITEM, mongo.collection(TOP_LEVEL_ITEM.tableName()),
                PROGRAMME_GROUP,mongo.collection(PROGRAMME_GROUP.tableName()),
                CHILD_ITEM, mongo.collection(CHILD_ITEM.tableName())
        );
    }
    
    public DBCollection collectionFor(ContentCategory category) {
        return tableMap.get(category);
    }
    
}