package org.atlasapi.persistence.cassandra;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import java.util.Map;

/**
 */
public class CassandraSchema {

    public static final String CLUSTER = "Atlas";
    //
    public static final String PROD_KEYSPACE = "Atlas";
    public static final String STAGE_KEYSPACE = "AtlasStage";
    public static final Map<String, String> KEYSPACES = ImmutableMap.<String, String>builder().put("prod", PROD_KEYSPACE).put("stage", STAGE_KEYSPACE).build();
    //
    public static final ColumnFamily<String, String> ITEMS_CF = new ColumnFamily<String, String>(
            "Item",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> CONTAINER_CF = new ColumnFamily<String, String>(
            "Container",
            StringSerializer.get(),
            StringSerializer.get());
    //
    public static final String ITEM_COLUMN = "item";
    public static final String CLIPS_COLUMN = "clips";
    public static final String VERSIONS_COLUMN = "versions";
    public static final String CONTAINER_SUMMARY_COLUMN = "container_summary";
    //
    public static final String CONTAINER_COLUMN = "container";
    public static final String CHILDREN_COLUMN = "children";

    public static String getKeyspace(String environment) {
        String ks = KEYSPACES.get(environment);
        if (ks != null) {
            return ks;
        } else {
            throw new IllegalArgumentException("Illegal environment parameter: " + environment);
        }
    }
}
