package org.atlasapi.persistence.topic.cassandra;

import com.google.common.collect.Iterables;
import java.util.Arrays;
import org.atlasapi.content.criteria.ContentQuery;
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
        Topic topic = new Topic(1l, "ns", "v1");
        topic.setCanonicalUri("u1");

        store.write(topic);

        assertEquals(topic, store.topicForId(1l).requireValue());
    }

    @Test
    public void testFindByNamespaceAndValue() {
        Topic topic = new Topic(1l, "ns", "v1");
        topic.setCanonicalUri("u1");

        store.write(topic);

        assertEquals(topic, store.topicFor("ns", "v1").requireValue());
    }

    @Test
    public void testFindNothing() {
        assertTrue(store.topicForId(0l).isNothing());
        assertTrue(store.topicFor("ns1", "v1").isNothing());
    }

    @Test
    public void testFindByIds() {
        Topic topic1 = new Topic(1l, "ns", "v1");
        topic1.setCanonicalUri("u1");
        Topic topic2 = new Topic(2l, "ns", "v2");
        topic2.setCanonicalUri("u2");

        store.write(topic1);
        store.write(topic2);

        assertEquals(2, Iterables.size(store.topicsForIds(Arrays.asList(1l, 2l))));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testContentQueryIsNotSupportedAsShouldBeImplementedViaElasticSearch() {
        store.topicsFor(ContentQuery.MATCHES_EVERYTHING);
    }
}
