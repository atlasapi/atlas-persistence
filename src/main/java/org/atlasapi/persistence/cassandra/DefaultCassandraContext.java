package org.atlasapi.persistence.cassandra;

import static org.atlasapi.persistence.cassandra.CassandraSchema.CLUSTER;
import static org.atlasapi.persistence.cassandra.CassandraSchema.KEYSPACE;

import java.util.List;

import com.google.common.base.Joiner;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class DefaultCassandraContext {

    private final AstyanaxContext<Keyspace> context;

    public DefaultCassandraContext(List<String> seeds, int port, int maxConnections, int connectionTimeout, int requestTimeout) {
        this.context = new AstyanaxContext.Builder().forCluster(CLUSTER).forKeyspace(KEYSPACE).
                withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE)).
                withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(CLUSTER).setPort(port).
                setMaxBlockedThreadsPerHost(maxConnections).
                setMaxConnsPerHost(maxConnections).
                setConnectTimeout(connectionTimeout).
                setSeeds(Joiner.on(",").join(seeds))).
                withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).
                buildKeyspace(ThriftFamilyFactory.getInstance());
    }
    
    public AstyanaxContext<Keyspace> get() {
        return this.context;
    }
}
