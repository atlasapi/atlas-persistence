package org.atlasapi.persistence.content.cassandra;

import com.google.common.collect.Sets;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import java.util.Arrays;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.junit.After;
import org.junit.Test;
import org.junit.Before;
import org.junit.Ignore;
import static org.atlasapi.persistence.cassandra.CassandraSchema.*;
import static org.junit.Assert.*;

/**
 */
@Ignore(value = "Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraContentStoreTest {

    //
    //private static final String CASSANDRA_HOST = "cassandra1.owl.atlas.mbst.tv";
    private static final String CASSANDRA_HOST = "127.0.0.1";
    //
    private AstyanaxContext context;
    private CassandraContentStore store;
    //

    @Before
    public void before() {
        context = new AstyanaxContext.Builder().forCluster(CLUSTER).forKeyspace(KEYSPACE).
                withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE)).
                withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(CLUSTER).setPort(9160).
                setMaxBlockedThreadsPerHost(Runtime.getRuntime().availableProcessors() * 10).
                setMaxConnsPerHost(Runtime.getRuntime().availableProcessors() * 10).
                setConnectTimeout(10000).
                setSeeds(CASSANDRA_HOST)).
                withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).
                buildKeyspace(ThriftFamilyFactory.getInstance());
        store = new CassandraContentStore(context, 10000);
        context.start();
        store.init();
    }

    @After
    public void close() {
        context.shutdown();
    }

    @Test
    public void testItems() {
        Item item1 = new Item("item1", "item1", Publisher.METABROADCAST);
        item1.setTitle("item1");
        item1.setId(1L);
        item1.setClips(Arrays.asList(new Clip("clip1", "clip1", Publisher.METABROADCAST)));
        item1.setVersions(Sets.newHashSet(new Version()));

        Item item2 = new Item("item2", "item2", Publisher.METABROADCAST);
        item2.setTitle("item2");
        item2.setId(2L);
        item2.setClips(Arrays.asList(new Clip("clip2", "clip2", Publisher.METABROADCAST)));
        item2.setVersions(Sets.newHashSet(new Version()));

        store.createOrUpdate(item1);
        store.createOrUpdate(item2);

        assertEquals(1, store.findByCanonicalUris(Arrays.asList("item1")).getAllResolvedResults().size());
        assertEquals(1, store.findByCanonicalUris(Arrays.asList("item2")).getAllResolvedResults().size());
    }

    @Test
    public void testContainerWithChild() {
        Item child1 = new Item("child1", "child1", Publisher.METABROADCAST);
        child1.setTitle("child1");
        child1.setId(3L);

        Container container1 = new Container("container1", "curie1", Publisher.METABROADCAST);
        container1.setTitle("container1");
        container1.setId(4L);

        container1.setChildRefs(Arrays.asList(child1.childRef()));
        child1.setParentRef(ParentRef.parentRefFrom(container1));

        store.createOrUpdate(child1);
        store.createOrUpdate(container1);

        assertEquals(1, store.findByCanonicalUris(Arrays.asList("container1")).getAllResolvedResults().size());
        assertEquals(1, store.findByCanonicalUris(Arrays.asList("child1")).getAllResolvedResults().size());

        container1.setTitle("container11");
        store.createOrUpdate(container1);
    }
}
