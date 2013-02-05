package org.atlasapi.persistence.content.elasticsearch;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.topic.Topic;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Iterables;


public class EsTopicIndexTest {

    private final Node esClient = NodeBuilder.nodeBuilder()
        .local(true).clusterName(UUID.randomUUID().toString())
        .build().start();
    private final String indexName = "topics";
    private final EsTopicIndex index = new EsTopicIndex(esClient, indexName, 60, TimeUnit.SECONDS);
    
    
    @BeforeClass
    public static void before() throws Exception {
        Logger root = Logger.getRootLogger();
        root.addAppender(new ConsoleAppender(
            new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
        root.setLevel(Level.WARN);
    }
    
    @Before
    public void setup() throws Exception {
        index.startAndWait();
    }
    
    @After
    public void after() throws Exception {
        esClient.client().admin().indices()
            .delete(Requests.deleteIndexRequest(indexName)).get();
        esClient.close();
    }
    
    @Test
    public void testIndexesAndRetrievesTopic() throws Exception {
        Topic topic = new Topic();
        topic.setId(Id.valueOf(1234));
        topic.setPublisher(Publisher.DBPEDIA);
        topic.addAlias("an:Alias");
        topic.setTitle("title");
        topic.setDescription("description");
        
        index.index(topic);
        
        Thread.sleep(1000);
        
        GetResponse got = esClient.client().get(Requests.getRequest(indexName).id("1234")).actionGet(10000);
        
        Map<String, Object> source = got.getSource();
        assertEquals(1234, source.get("aid"));
        assertEquals("dbpedia.org", source.get("source"));
        assertEquals("title", source.get("title"));
        assertEquals("description", source.get("description"));
        
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> aliases = (List<Map<String, Object>>) source.get("alias");
        assertEquals("an:Alias", Iterables.getOnlyElement(aliases).get("value"));
    }

}
