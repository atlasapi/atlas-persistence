package org.atlasapi.persistence;

import static org.atlasapi.persistence.cassandra.CassandraSchema.CLUSTER;

import org.atlasapi.equiv.CassandraEquivalenceSummaryStore;
import org.atlasapi.persistence.cassandra.CassandraSchema;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;

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
    private final CassandraEquivalenceSummaryStore cassandraEquivalenceSummaryStore;

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
        this.cassandraContentStore = new CassandraContentStore(cassandraContext, requestTimeout);
        this.cassandraEquivalenceSummaryStore = new CassandraEquivalenceSummaryStore(cassandraContext, requestTimeout);
        //
        cassandraContext.start();
        cassandraContentStore.init();
    }

    public void destroy() {
        cassandraContext.shutdown();
    }

    public CassandraContentStore cassandraContentStore() {
        return cassandraContentStore;
    }
    
    public void init() {
        cassandraContentStore.init();
    }
    
    public CassandraEquivalenceSummaryStore cassandraEquivalenceSummaryStore() {
        return cassandraEquivalenceSummaryStore;
    }
}
