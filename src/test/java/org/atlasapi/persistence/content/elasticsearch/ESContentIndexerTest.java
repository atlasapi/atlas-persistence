package org.atlasapi.persistence.content.elasticsearch;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.elasticsearch.schema.ESSchema;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHits;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 */
public class ESContentIndexerTest {

    private Node esNode;

    @Before
    public void before() throws Exception {
        esNode = NodeBuilder.nodeBuilder().
                clusterName(ESSchema.CLUSTER_NAME).
                settings(ImmutableSettings.settingsBuilder().
                put("transport.tcp.port", 9300).
                put("discovery.zen.ping.multicast.enabled", false).
                put("discovery.zen.ping.unicast", "localhost:9300")).
                build().
                start();
    }

    @After
    public void after() throws Exception {
        esNode.client().admin().indices().delete(Requests.deleteIndexRequest(ESSchema.INDEX_NAME));
        esNode.close();
    }

    @Test
    public void testIndex() throws IOException, InterruptedException {
        Broadcast broadcast = new Broadcast("MB", new DateTime(), new DateTime().plusHours(1));
        Version version = new Version();
        Item item = new Item("uri", "curie", Publisher.METABROADCAST);
        version.addBroadcast(broadcast);
        item.addVersion(version);

        ESContentIndexer contentIndexer = new ESContentIndexer("localhost:9300", 60000);
        contentIndexer.init();
        
        Thread.sleep(1000);
        
        contentIndexer.index(item);

        Thread.sleep(1000);

        ListenableActionFuture<SearchResponse> result1 = esNode.client().prepareSearch(ESSchema.INDEX_NAME).setQuery(
                QueryBuilders.nestedQuery("broadcasts", QueryBuilders.fieldQuery("channel", "MB"))).execute();
        SearchHits hits1 = result1.actionGet(60, TimeUnit.SECONDS).getHits();
        assertEquals(1, hits1.totalHits());

        ListenableActionFuture<SearchResponse> result2 = esNode.client().prepareSearch(ESSchema.INDEX_NAME).setQuery(
                QueryBuilders.nestedQuery("broadcasts", QueryBuilders.rangeQuery("transmissionTime").from(new DateTime().minusDays(1).toDate()))).execute();
        SearchHits hits2 = result2.actionGet(60, TimeUnit.SECONDS).getHits();
        assertEquals(1, hits2.totalHits());

        ListenableActionFuture<SearchResponse> result3 = esNode.client().prepareSearch(ESSchema.INDEX_NAME).setQuery(
                QueryBuilders.nestedQuery("broadcasts", QueryBuilders.rangeQuery("transmissionTime").from(new DateTime().toDate()))).execute();
        SearchHits hits3 = result3.actionGet(60, TimeUnit.SECONDS).getHits();
        assertEquals(0, hits3.totalHits());
    }
}
