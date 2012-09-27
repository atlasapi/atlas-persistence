package org.atlasapi.equiv;

import static org.atlasapi.persistence.cassandra.CassandraSchema.CLUSTER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.collect.OptionalMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.junit.Ignore;

@Ignore(value = "Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraEquivalenceSummaryStoreTest {

    private CassandraEquivalenceSummaryStore store;
    
    private static final String CASSANDRA_HOST = "127.0.0.1";

    private AstyanaxContext<Keyspace> context;
    
    @Before
    public void setup() {
        context = new AstyanaxContext.Builder()
            .forCluster(CLUSTER)
            .forKeyspace("AtlasTest")
            .withAstyanaxConfiguration(
                new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE)
            )
            .withConnectionPoolConfiguration(
                new ConnectionPoolConfigurationImpl(CLUSTER)
                    .setPort(9160)
                    .setMaxBlockedThreadsPerHost(Runtime.getRuntime().availableProcessors() * 10)
                    .setMaxConnsPerHost(Runtime.getRuntime().availableProcessors() * 10)
                    .setConnectTimeout(10000)
                    .setSeeds(CASSANDRA_HOST)
            )
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
        store = new CassandraEquivalenceSummaryStore(context, 1000);
    }
    
    @After
    public void cleardown() throws OperationException, ConnectionException {
        context.getEntity().truncateColumnFamily(CassandraEquivalenceSummaryStore.EQUIV_SUM_CF);
    }
    
    @Test
    public void testStoresAndResolvesSummaries() {
        
        EquivalenceSummary summaryOne = summaryWithKey("one", null);
        EquivalenceSummary summaryTwo = summaryWithKey("two", "parent");
        
        store.store(summaryOne);
        
        OptionalMap<String, EquivalenceSummary> resolved = store.summariesForUris(ImmutableSet.of("one","two"));
        
        assertThat(resolved.get("one").get(), is(equalTo(summaryOne)));
        assertThat(resolved.get("two").isPresent(), is(false));
        
        store.store(summaryTwo);
        
        resolved = store.summariesForUris(ImmutableSet.of("one","two"));

        assertThat(resolved.get("two").get(), is(equalTo(summaryTwo)));
        assertThat(resolved.get("one").get(), is(equalTo(summaryOne)));
        
        Set<EquivalenceSummary> childSummaries = store.summariesForChildren("parent");
        assertThat(childSummaries.size(), is(1));
        assertThat(Iterables.getOnlyElement(childSummaries).getSubject(), is(summaryTwo.getSubject()));
        
    }

    private EquivalenceSummary summaryWithKey(String key, String parent) {
        String subject = key;
        Iterable<String> candidates = ImmutableSet.of(key);
        ImmutableMap<Publisher, ContentRef> equivalents = ImmutableMap.of();
        return new EquivalenceSummary(subject, parent, candidates, equivalents);
    }

}
