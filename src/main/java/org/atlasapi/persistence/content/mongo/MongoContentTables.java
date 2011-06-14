package org.atlasapi.persistence.content.mongo;

import org.atlasapi.persistence.content.ContentTable;

import com.google.common.collect.ImmutableMap;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;

public class MongoContentTables {

    private ImmutableMap<ContentTable, DBCollection> tableMap;

    public MongoContentTables(DatabasedMongo mongo) {
        this.tableMap = ImmutableMap.of(
                ContentTable.TOP_LEVEL_CONTAINERS, mongo.collection("containers"),
                ContentTable.TOP_LEVEL_ITEMS, mongo.collection("topLevelItems"),
                ContentTable.PROGRAMME_GROUPS,mongo.collection("programmeGroups"),
                ContentTable.CHILD_ITEMS, mongo.collection("children")
        );
    }
    
    public DBCollection collectionFor(ContentTable table) {
        return tableMap.get(table);
    }
    
}
