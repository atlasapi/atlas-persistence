package org.atlasapi.persistence;

import static org.atlasapi.persistence.cassandra.CassandraSchema.CLUSTER;

import org.atlasapi.equiv.CassandraEquivalenceSummaryStore;
import org.atlasapi.persistence.cassandra.CassandraSchema;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.atlasapi.persistence.lookup.cassandra.CassandraLookupEntryStore;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 */
public class CassandraContentPersistenceModule {

    private final AstyanaxContext<Keyspace> cassandraContext;
    private final CassandraContentStore cassandraContentStore;
    private final CassandraLookupEntryStore lookupEntryStore;
    private final CassandraEquivalenceSummaryStore equivalenceSummaryStore;

    public CassandraContentPersistenceModule(String environment, String seeds, int port, int connectionTimeout, int requestTimeout) {
        this.cassandraContext = new AstyanaxContext.Builder().forCluster(CLUSTER).forKeyspace(CassandraSchema.getKeyspace(environment)).
                withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE)).
                withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(CLUSTER).setPort(port).
                setMaxBlockedThreadsPerHost(Runtime.getRuntime().availableProcessors() * 10).
                setMaxConnsPerHost(Runtime.getRuntime().availableProcessors() * 10).
                setConnectTimeout(connectionTimeout).
                setSeeds(seeds)).
                withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).
                buildKeyspace(ThriftFamilyFactory.getInstance());
        this.lookupEntryStore = new CassandraLookupEntryStore(cassandraContext, requestTimeout);
        this.cassandraContentStore = new CassandraContentStore(cassandraContext, requestTimeout, lookupEntryStore);
        this.equivalenceSummaryStore = new CassandraEquivalenceSummaryStore(cassandraContext, requestTimeout);

    }

    public void init() {
        cassandraContext.start();
        cassandraContentStore.init();
    }

    public void destroy() {
        cassandraContext.shutdown();
    }

    public CassandraContentStore cassandraContentStore() {
        return cassandraContentStore;
    }
    
    public CassandraLookupEntryStore cassandraLookupEntryStore() {
        return lookupEntryStore;
    }
    
    public CassandraEquivalenceSummaryStore cassandraEquivalenceSummaryStore() {
        return  equivalenceSummaryStore;
    }
}
