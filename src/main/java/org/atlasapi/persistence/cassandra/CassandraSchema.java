package org.atlasapi.persistence.cassandra;

import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

public class CassandraSchema {

    public static final ColumnFamily<String, String> ITEMS_CF = new ColumnFamily<String, String>(
            "Item",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> CONTAINER_CF = new ColumnFamily<String, String>(
            "Container",
            StringSerializer.get(),
            StringSerializer.get());
    public static final String ITEM_COLUMN = "item";
    public static final String CLIPS_COLUMN = "clips";
    public static final String VERSIONS_COLUMN = "versions";
    public static final String CONTAINER_COLUMN = "container";
    public static final String CHILDREN_COLUMN = "children";
}
