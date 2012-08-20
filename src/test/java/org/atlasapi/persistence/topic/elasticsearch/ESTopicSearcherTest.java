package org.atlasapi.persistence.topic.elasticsearch;

import com.metabroadcast.common.base.Maybe;
import org.atlasapi.persistence.content.elasticsearch.*;
import java.io.IOException;
import java.util.List;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.elasticsearch.schema.ESSchema;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicSearcher;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 */
public class ESTopicSearcherTest {

    private Node esClient;
    private ESContentIndexer indexer;

    @Before
    public void before() throws Exception {
        esClient = NodeBuilder.nodeBuilder().local(true).clusterName(ESSchema.CLUSTER_NAME).build().start();
        indexer = new ESContentIndexer(esClient, 60000);
        indexer.init();
        Thread.sleep(1000);
    }

    @After
    public void after() throws Exception {
        esClient.client().admin().indices().delete(Requests.deleteIndexRequest(ESSchema.INDEX_NAME));
        esClient.close();
        Thread.sleep(1000);
    }

    @Test
    public void testPopularTopics() throws IOException, InterruptedException {
        Broadcast broadcast1 = new Broadcast("MB", new DateTime(), new DateTime().plusHours(1));
        Version version1 = new Version();
        Broadcast broadcast2 = new Broadcast("MB", new DateTime().plusHours(2), new DateTime().plusHours(3));
        Version version2 = new Version();
        version1.addBroadcast(broadcast1);
        version2.addBroadcast(broadcast2);
        //
        TopicRef topic1 = new TopicRef(1l, 1.0f, Boolean.TRUE, TopicRef.Relationship.ABOUT);
        TopicRef topic2 = new TopicRef(2l, 1.0f, Boolean.TRUE, TopicRef.Relationship.ABOUT);
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
        //
        indexer.index(item1);
        indexer.index(item2);
        indexer.index(item3);
        //
        Thread.sleep(1000);
        //
        TopicQueryResolver resolver = mock(TopicQueryResolver.class);
        when(resolver.topicForId(1l)).thenReturn(Maybe.just(new Topic(1l)));
        when(resolver.topicForId(2l)).thenReturn(Maybe.just(new Topic(2l)));
        //
        TopicSearcher searcher = new ESTopicSearcher(esClient, 60000);
        List<Topic> topics = searcher.popularTopics(new Interval(new DateTime().minusHours(1), new DateTime().plusHours(1)), resolver);
        assertEquals(Long.valueOf(1), topics.get(0).getId());
        assertEquals(Long.valueOf(2), topics.get(1).getId());
    }
}
