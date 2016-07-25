package org.atlasapi.equiv;

import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.collect.OptionalMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.OperationException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.cql.CqlStatement;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@Ignore(value="Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraEquivalenceSummaryStoreTest {

    private CassandraEquivalenceSummaryStore store;
    
    // TODO move these out to an environment property
    private static final String CLUSTER_NAME = "Build";
    private static final String KEYSPACE_NAME = "Atlas";
    private static final String SEEDS = "localhost";
    private static final int CLIENT_THREADS = 5;
    private static final int CONNECTION_TIMEOUT = 1000;
    private static final int PORT = 9160;
    private CqlStatement cqlStatement;

    private AstyanaxContext<Keyspace> context;
    
    @Before
    public void setup() throws Exception {
        context = new AstyanaxContext.Builder()
                .forCluster(CLUSTER_NAME)
                .forKeyspace(KEYSPACE_NAME)
                .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                        .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
                        .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN)
                        .setCqlVersion("3.0.0")
                        .setTargetCassandraVersion("1.2")
                )
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("Atlas")
                        .setPort(PORT)
                        .setMaxBlockedThreadsPerHost(CLIENT_THREADS)
                        .setMaxConnsPerHost(CLIENT_THREADS)
                        .setMaxConns(CLIENT_THREADS * 5)
                        .setConnectTimeout(CONNECTION_TIMEOUT)
                        .setSeeds(SEEDS)
                )
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
        cqlStatement = context.getClient().prepareCqlStatement();
        store = new CassandraEquivalenceSummaryStore(context, 1000);
    }
    
    @After
    public void cleardown() throws OperationException, ConnectionException {
        context.getEntity().truncateColumnFamily(CassandraEquivalenceSummaryStore.EQUIV_SUM_CF);
    }

    @Test
    public void testLegacyContentWithEquivalentFromOnePublisher() throws ConnectionException {
        cqlStatement.withCql("INSERT INTO \"EquivalenceSummaries\" (key, column1, value) "
                + "VALUES ( 'three', 'summary', '{\"subject\":\"three\", \"parent\":\"parent\", \"candidates\":[\"one\"],\"equivalents\":{\"bbc.co.uk\":{\"canonicalUri\":\"uri\",\"publisher\":\"pressassociation.com\",\"parentUri\":\"parent\"}}}')");
        cqlStatement.execute();
        OptionalMap<String, EquivalenceSummary> resolved = store.summariesForUris(ImmutableSet.of("three"));
        EquivalenceSummary summary = resolved.get("three").get();
        assertThat(summary.getSubject(), is(equalTo("three")));
        assertThat(summary.getParent(), is(equalTo("parent")));
        assertThat(summary.getCandidates(), is(equalTo(ImmutableList.of("one"))));
        assertThat(summary.getEquivalents().values().asList().get(0).getCanonicalUri(), is(equalTo("uri")));
        assertThat(summary.getEquivalents().values().asList().get(0).getParentUri(), is(equalTo("parent")));
        assertThat(summary.getEquivalents().values().asList().get(0).getPublisher(), is(equalTo(Publisher.PA)));
    }

    @Test
    public void testLegacyContentWithEquivalentFromOnePublisherWithNoCandidates() throws ConnectionException {
        cqlStatement.withCql("INSERT INTO \"EquivalenceSummaries\" (key, column1, value) "
                + "VALUES ( 'three', 'summary', '{\"subject\":\"three\",\"candidates\":[],\"equivalents\":{\"bbc.co.uk\":{\"canonicalUri\":\"uri\",\"publisher\":\"pressassociation.com\",\"parentUri\":\"parent\"}}}')");
        cqlStatement.execute();
        OptionalMap<String, EquivalenceSummary> resolved = store.summariesForUris(ImmutableSet.of("three"));
        EquivalenceSummary summary = resolved.get("three").get();
        assertThat(summary.getSubject(), is(equalTo("three")));
        assertThat(summary.getCandidates(), is(equalTo(ImmutableList.of())));
        assertThat(summary.getEquivalents().values().asList().get(0).getCanonicalUri(), is(equalTo("uri")));
        assertThat(summary.getEquivalents().values().asList().get(0).getParentUri(), is(equalTo("parent")));
        assertThat(summary.getEquivalents().values().asList().get(0).getPublisher(), is(equalTo(Publisher.PA)));
    }

    @Test
    public void testLegacyContentWithNoEquivalents() throws ConnectionException {
        cqlStatement.withCql("INSERT INTO \"EquivalenceSummaries\" (key, column1, value) "
                + "VALUES ( 'three', 'summary', '{\"subject\":\"three\",\"candidates\":[],\"equivalents\":{}}')");
        cqlStatement.execute();
        OptionalMap<String, EquivalenceSummary> resolved = store.summariesForUris(ImmutableSet.of("three"));
        EquivalenceSummary summary = resolved.get("three").get();
        assertThat(summary.getSubject(), is(equalTo("three")));
        assertThat(summary.getCandidates(), is(equalTo(ImmutableList.of())));
        assertThat(summary.getEquivalents().values().asList(), is(ImmutableList.of()));
    }

    @Test
    public void testStoresAndResolvesSummaries() {
        
        ImmutableMultimap<Publisher, ContentRef> refs = ImmutableMultimap.<Publisher, ContentRef>of(
            Publisher.BBC, new ContentRef("uri", Publisher.PA, "parent")
        );
        EquivalenceSummary summaryOne = new EquivalenceSummary("one", null, ImmutableSet.of("one"), refs);
        EquivalenceSummary summaryTwo = new EquivalenceSummary("two", "parent", ImmutableSet.of("two"), refs);
        
        store.store(summaryOne);
        
        OptionalMap<String, EquivalenceSummary> resolved = store.summariesForUris(ImmutableSet.of("one","two"));
        
        assertThat(resolved.get("one").get(), is(equalTo(summaryOne)));
        assertThat(resolved.get("two").isPresent(), is(false));
        
        store.store(summaryTwo);
        
        resolved = store.summariesForUris(ImmutableSet.of("one","two"));

        assertThat(resolved.get("two").get(), is(equalTo(summaryTwo)));
        assertThat(resolved.get("one").get(), is(equalTo(summaryOne)));
        
    }

    @Test
    public void testStoresAndResolvesSummariesWithSeveralEquivalentsFromSamePublisher() {

        ImmutableMultimap<Publisher, ContentRef> refs = ImmutableMultimap.<Publisher, ContentRef>of(
                Publisher.BBC, new ContentRef("uri", Publisher.PA, "parent"),
                Publisher.BBC, new ContentRef("uri2", Publisher.PA, "parent2"),
                Publisher.PA, new ContentRef("uri3", Publisher.AMAZON_UK, "parent3"),
                Publisher.PA, new ContentRef("uri4", Publisher.ADAPT_BBC_PODCASTS, "parent4"),
                Publisher.VF_C5, new ContentRef("uri5", Publisher.ARQIVA, "parent5")
        );

        EquivalenceSummary summaryOne = new EquivalenceSummary(
                "one",
                null,
                ImmutableSet.of("one"),
                refs
        );
        store.store(summaryOne);
        OptionalMap<String, EquivalenceSummary> resolved = store.summariesForUris(ImmutableSet.of("one"));

        assertThat(resolved.get("one").get(), is(equalTo(summaryOne)));

    }

}
