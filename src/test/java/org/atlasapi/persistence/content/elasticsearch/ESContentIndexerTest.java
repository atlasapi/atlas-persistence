package org.atlasapi.persistence.content.elasticsearch;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.elasticsearch.schema.ESSchema;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacet.Entry;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 */
public class ESContentIndexerTest {
    
    private Node esClient;

    @Before
    public void before() throws Exception {
        esClient = NodeBuilder.nodeBuilder().local(true).clusterName(ESSchema.CLUSTER_NAME).build().start();
    }
    
    @After
    public void after() throws Exception {
        esClient.client().admin().indices().delete(Requests.deleteIndexRequest(ESSchema.INDEX_NAME));
        esClient.close();
        Thread.sleep(1000);
    }

    @Test
    public void testScheduleQueries() throws IOException, InterruptedException {
        Broadcast broadcast = new Broadcast("MB", new DateTime(), new DateTime().plusHours(1));
        Version version = new Version();
        Item item = new Item("uri", "curie", Publisher.METABROADCAST);
        version.addBroadcast(broadcast);
        item.addVersion(version);
        
        ESContentIndexer contentIndexer = new ESContentIndexer(esClient);
        contentIndexer.init();
        
        Thread.sleep(1000);
        
        contentIndexer.index(item);
        
        Thread.sleep(1000);
        
        ListenableActionFuture<SearchResponse> result1 = esClient.client().prepareSearch(ESSchema.INDEX_NAME).setQuery(
                QueryBuilders.nestedQuery("broadcasts", QueryBuilders.fieldQuery("channel", "MB"))).execute();
        SearchHits hits1 = result1.actionGet(60, TimeUnit.SECONDS).getHits();
        assertEquals(1, hits1.totalHits());
        
        ListenableActionFuture<SearchResponse> result2 = esClient.client().prepareSearch(ESSchema.INDEX_NAME).setQuery(
                QueryBuilders.nestedQuery("broadcasts", QueryBuilders.rangeQuery("transmissionTime").from(new DateTime().minusDays(1).toDate()))).execute();
        SearchHits hits2 = result2.actionGet(60, TimeUnit.SECONDS).getHits();
        assertEquals(1, hits2.totalHits());
        
        ListenableActionFuture<SearchResponse> result3 = esClient.client().prepareSearch(ESSchema.INDEX_NAME).setQuery(
                QueryBuilders.nestedQuery("broadcasts", QueryBuilders.rangeQuery("transmissionTime").from(new DateTime().toDate()))).execute();
        SearchHits hits3 = result3.actionGet(60, TimeUnit.SECONDS).getHits();
        assertEquals(0, hits3.totalHits());
    }
    
    @Test
    public void testTopicFacets() throws IOException, InterruptedException {
        Broadcast broadcast1 = new Broadcast("MB", new DateTime(), new DateTime().plusHours(1));
        Version version1 = new Version();
        Broadcast broadcast2 = new Broadcast("MB", new DateTime().plusHours(2), new DateTime().plusHours(3));
        Version version2 = new Version();
        version1.addBroadcast(broadcast1);
        version2.addBroadcast(broadcast2);
        //
        TopicRef topic1 = new TopicRef(1l, 1.0f, Boolean.TRUE);
        TopicRef topic2 = new TopicRef(2l, 1.0f, Boolean.TRUE);
        //
        Item item1 = new Item("uri1", "curie1", Publisher.METABROADCAST);
        item1.addVersion(version1);
        item1.addTopicRef(topic1);
        item1.addTopicRef(topic2);
        Item item2 = new Item("uri2", "curie2", Publisher.METABROADCAST);
        item2.addVersion(version1);
        item2.addTopicRef(topic1);
        Item item3 = new Item("uri3", "curie3", Publisher.METABROADCAST);
        item3.addVersion(version2);
        item3.addTopicRef(topic2);
        
        ESContentIndexer contentIndexer = new ESContentIndexer(esClient);
        contentIndexer.init();
        
        Thread.sleep(1000);
        
        contentIndexer.index(item1);
        contentIndexer.index(item2);
        contentIndexer.index(item3);
        
        Thread.sleep(1000);
        
        ListenableActionFuture<SearchResponse> result = esClient.client().prepareSearch(ESSchema.INDEX_NAME).setQuery(
                QueryBuilders.nestedQuery("broadcasts", QueryBuilders.rangeQuery("transmissionTime").from(new DateTime().minusHours(1).toDate()).to(new DateTime().plusHours(1).toDate()))).
                addFacet(FacetBuilders.termsFacet("topics").field("topics.id")).
                execute();
        Facets facets = result.actionGet(60, TimeUnit.SECONDS).getFacets();
        List<? extends Entry> terms = facets.facet(TermsFacet.class, "topics").entries();
        assertEquals(2, terms.size());
        assertEquals("1", terms.get(0).getTerm());
        assertEquals(2, terms.get(0).getCount());
        assertEquals("2", terms.get(1).getTerm());
        assertEquals(1, terms.get(1).getCount());
    }
}
