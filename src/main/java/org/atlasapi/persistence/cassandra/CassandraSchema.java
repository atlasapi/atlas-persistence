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
    public static final ColumnFamily<String, String> PEOPLE_CF = new ColumnFamily<String, String>(
            "Person",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> CONTENT_GROUP_CF = new ColumnFamily<String, String>(
            "ContentGroup",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> CONTENT_GROUP_SECONDARY_CF = new ColumnFamily<String, String>(
            "ContentGroupSecondary",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> CHANNEL_CF = new ColumnFamily<String, String>(
            "Channel",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> CHANNEL_GROUP_CF = new ColumnFamily<String, String>(
            "ChannelGroup",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> CHANNEL_GROUP_SECONDARY_CF = new ColumnFamily<String, String>(
            "ChannelGroupSecondary",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> PRODUCT_CF = new ColumnFamily<String, String>(
            "Product",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> PRODUCT_DIRECT_SECONDARY_CF = new ColumnFamily<String, String>(
            "ProductDirectSecondary",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> PRODUCT_INVERTED_SECONDARY_CF = new ColumnFamily<String, String>(
            "ProductInvertedSecondary",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> SEGMENT_CF = new ColumnFamily<String, String>(
            "Segment",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> SEGMENT_SECONDARY_CF = new ColumnFamily<String, String>(
            "SegmentSecondary",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> TOPIC_CF = new ColumnFamily<String, String>(
            "Topic",
            StringSerializer.get(),
            StringSerializer.get());
    public static final ColumnFamily<String, String> TOPIC_SECONDARY_CF = new ColumnFamily<String, String>(
            "TopicSecondary",
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
    //
    public static final String PERSON_COLUMN = "person";
    public static final String CONTENT_GROUP_COLUMN = "content_group";
    public static final String CONTENTS_COLUMN = "contents";
    //
    public static final String CHANNEL_COLUMN = "channel";
    //
    public static final String CHANNEL_GROUP_COLUMN = "channel_group";
    //
    public static final String PRODUCT_COLUMN = "product";
    //
    public static final String SEGMENT_COLUMN = "segment";
    //
    public static final String TOPIC_COLUMN = "topic";

    public static String getKeyspace(String environment) {
        String ks = KEYSPACES.get(environment);
        if (ks != null) {
            return ks;
        } else {
            throw new IllegalArgumentException("Illegal environment parameter: " + environment);
        }
    }
}
