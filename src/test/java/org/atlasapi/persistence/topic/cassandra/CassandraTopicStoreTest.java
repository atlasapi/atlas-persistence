package org.atlasapi.persistence.topic.cassandra;

import com.google.common.collect.Iterables;
import java.util.Arrays;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.cassandra.BaseCassandraTest;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;

@Ignore(value = "Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraTopicStoreTest extends BaseCassandraTest {

    private CassandraTopicStore store;

    @Before
    @Override
    public void before() {
        super.before();
        store = new CassandraTopicStore(context, 10000);
    }

    @Test
    public void testFindById() {
        Topic topic = new Topic(Id.valueOf(1), "ns", "v1");
        topic.setCanonicalUri("u1");

        store.write(topic);

        assertEquals(topic, store.topicForId(Id.valueOf(1l)).requireValue());
    }

    @Test
    public void testFindByNamespaceAndValue() {
        Topic topic = new Topic(Id.valueOf(1), "ns", "v1");
        topic.setCanonicalUri("u1");

        store.write(topic);

        assertEquals(topic, store.topicFor("ns", "v1").requireValue());
    }
    
    @Test
    public void testFindByPublisherNamespaceAndValue() {
        Topic topic = new Topic(Id.valueOf(1), "ns", "v1");
        topic.setCanonicalUri("u1");
        topic.setPublisher(Publisher.METABROADCAST);

        store.write(topic);

        assertEquals(topic, store.topicFor(Publisher.METABROADCAST, "ns", "v1").requireValue());
    }

    @Test
    public void testFindNothing() {
        assertTrue(store.topicForId(Id.valueOf(0l)).isNothing());
        assertTrue(store.topicFor("notfound", "v1").isNothing());
        assertTrue(store.topicFor(Publisher.METABROADCAST, "notfound", "v1").isNothing());
    }

    @Test
    public void testFindByIds() {
        Topic topic1 = new Topic(Id.valueOf(1), "ns", "v1");
        topic1.setCanonicalUri("u1");
        Topic topic2 = new Topic(Id.valueOf(2), "ns", "v2");
        topic2.setCanonicalUri("u2");

        store.write(topic1);
        store.write(topic2);

        assertEquals(2, Iterables.size(store.topicsForIds(Arrays.asList(Id.valueOf(1l), Id.valueOf(2l)))));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testContentQueryIsNotSupportedAsShouldBeImplementedViaElasticSearch() {
        store.topicsFor(ContentQuery.MATCHES_EVERYTHING);
    }
}
