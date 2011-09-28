package org.atlasapi.persistence.content.mongo;

import org.atlasapi.persistence.content.ContentCategory;

import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;

public class MongoContentTables {

    private ImmutableMap<ContentCategory, DBCollection> tableMap;

    public MongoContentTables(DatabasedMongo mongo) {
        this.tableMap = ImmutableMap.of(
                ContentCategory.CONTAINER, mongo.collection("containers"),
                ContentCategory.TOP_LEVEL_ITEM, mongo.collection("topLevelItems"),
                ContentCategory.PROGRAMME_GROUP,mongo.collection("programmeGroups"),
                ContentCategory.CHILD_ITEM, mongo.collection("children")
        );
    }
    
    public DBCollection collectionFor(ContentCategory category) {
        return tableMap.get(category);
    }
    
}
