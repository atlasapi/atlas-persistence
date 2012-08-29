package org.atlasapi.persistence.cassandra;

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 */
public interface CassandraSchema {

    static final String CLUSTER = "Atlas";
    //
    static final String KEYSPACE = "Atlas";
    //
    static final ColumnFamily<String, String> ITEMS_CF = new ColumnFamily<String, String>(
            "Item",
            StringSerializer.get(),
            StringSerializer.get());
    static final ColumnFamily<String, String> CONTAINER_CF = new ColumnFamily<String, String>(
            "Container",
            StringSerializer.get(),
            StringSerializer.get());
    //
    static final String ITEM_COLUMN = "item";
    static final String CLIPS_COLUMN = "clips";
    static final String VERSIONS_COLUMN = "versions";
    static final String DISPLAY_TITLE_COLUMN = "display_title";
    //
    static final String CONTAINER_COLUMN = "container";
    static final String CHILDREN_COLUMN = "children";
}
