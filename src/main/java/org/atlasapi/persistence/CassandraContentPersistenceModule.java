package org.atlasapi.persistence;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import static org.atlasapi.persistence.cassandra.CassandraSchema.*;

/**
 */
public class CassandraContentPersistenceModule {

    private final AstyanaxContext<Keyspace> cassandraContext;
    private final CassandraContentStore cassandraContentStore;

    public CassandraContentPersistenceModule(String seeds, int port, int connectionTimeout, int requestTimeout) {
        this.cassandraContext = new AstyanaxContext.Builder().forCluster(CLUSTER).forKeyspace(KEYSPACE).
                withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE)).
                withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(CLUSTER).setPort(port).
                setMaxBlockedThreadsPerHost(Runtime.getRuntime().availableProcessors() * 10).
                setMaxConnsPerHost(Runtime.getRuntime().availableProcessors() * 10).
                setConnectTimeout(connectionTimeout).
                setSeeds(seeds)).
                withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).
                buildKeyspace(ThriftFamilyFactory.getInstance());
        this.cassandraContentStore = new CassandraContentStore(cassandraContext, requestTimeout);
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
}
