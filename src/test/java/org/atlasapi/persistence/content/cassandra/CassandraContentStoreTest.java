package org.atlasapi.persistence.content.cassandra;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
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
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Ignore;

//@Ignore(value="Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraContentStoreTest {
    
    //private static final String CASSANDRA_HOST = "cassandra1.owl.atlas.mbst.tv";
    // TODO move these to test environment properties
    private static final String CASSANDRA_HOST = "127.0.0.1";
    private static final String CLUSTER = "Build";
    private static final String KEYSPACE = "Atlas";
    private CassandraContentStore store;
    
    @Before
    public void before() {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
            .forCluster(CLUSTER)
            .forKeyspace(KEYSPACE)
            .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
            .withConnectionPoolConfiguration(
                new ConnectionPoolConfigurationImpl(CLUSTER)
                    .setPort(9160)
                    .setMaxBlockedThreadsPerHost(10)
                    .setMaxConnsPerHost(10)
                    .setConnectTimeout(10000)
                    .setSeeds(CASSANDRA_HOST)
                )
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());
        
        store = new CassandraContentStore(context, 10000);
        store.init();
    }
    
    @After
    public void after() {
        store.close();
    }

    @Test
    public void testOperations() {
        Item item1 = new Item("item1", "item1", Publisher.METABROADCAST);
        item1.setId(1L);
        item1.setClips(Arrays.asList(new Clip("clip1", "clip1", Publisher.METABROADCAST)));
        item1.setVersions(Sets.newHashSet(new Version()));
        
        Item item2 = new Item("item2", "item2", Publisher.METABROADCAST);
        item2.setId(2L);
        item2.setClips(Arrays.asList(new Clip("clip2", "clip2", Publisher.METABROADCAST)));
        item2.setVersions(Sets.newHashSet(new Version()));
        
        Item child1 = new Item("child1", "child1", Publisher.METABROADCAST);
        child1.setId(3L);
        Container container1 = new Container("container1", "curie1", Publisher.METABROADCAST);
        container1.setId(4L);
        container1.setChildRefs(Arrays.asList(child1.childRef()));
        child1.setParentRef(ParentRef.parentRefFrom(container1));
        
        store.createOrUpdate(item1);
        store.createOrUpdate(item2);
        store.createOrUpdate(container1);
        store.createOrUpdate(child1);
        
        assertEquals(1, store.findByCanonicalUris(Arrays.asList("item1")).getAllResolvedResults().size());
        assertEquals(1, store.findByCanonicalUris(Arrays.asList("item2")).getAllResolvedResults().size());
        assertEquals(1, store.findByCanonicalUris(Arrays.asList("container1")).getAllResolvedResults().size());
        assertEquals(1, store.findByCanonicalUris(Arrays.asList("child1")).getAllResolvedResults().size());
        assertEquals(3, Iterators.size(store.listContent(ContentListingCriteria.defaultCriteria().forContent(Lists.newArrayList(ContentCategory.ITEMS)).build())));
        assertEquals(1, Iterators.size(store.listContent(ContentListingCriteria.defaultCriteria().forContent(Lists.newArrayList(ContentCategory.CONTAINERS)).build())));
    }
}
