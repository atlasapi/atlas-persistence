package org.atlasapi.persistence;

import org.atlasapi.equiv.CassandraEquivalenceSummaryStore;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.metabroadcast.common.properties.Configurer;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 */
@Configuration
public class CassandraPersistenceModule {

    private final String cluster = Configurer.get("cassandra.cluster").get();
    private final String keyspace = Configurer.get("cassandra.keyspace").get();
    private final String seeds = Configurer.get("cassandra.seeds").get();
    private final int port = Configurer.get("cassandra.port").toInt();
    private final int maxConnectionsPerHost = Configurer.get("cassandra.maxConnectionsPerHost").toInt();
    private final int maxBlockedThreadsPerHost = Configurer.get("cassandra.maxBlockedThreadsPerHost").toInt();
    private final int connectionTimeout = Configurer.get("cassandra.connectionTimeout").toInt();
    private final int requestTimeout = Configurer.get("cassandra.requestTimeout").toInt();

    @Bean
    @Qualifier(value = "cassandra")
    public CassandraContentStore cassandraContentStore() {
        return new CassandraContentStore(astyanaxContext(), requestTimeout);
    }
    
    @Bean
    @Qualifier(value = "cassandra")
    public CassandraEquivalenceSummaryStore cassandraEquivalenceSummaryStore() {
        return new CassandraEquivalenceSummaryStore(astyanaxContext(), requestTimeout);
    }
    
    @Bean
    @Qualifier(value = "cassandra")
    public AstyanaxContext<Keyspace> astyanaxContext() {
        return new AstyanaxContext.Builder()
            .forCluster(cluster)
            .forKeyspace(keyspace)
            .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
            .withConnectionPoolConfiguration(
                new ConnectionPoolConfigurationImpl(cluster)
                    .setPort(port)
                    .setMaxBlockedThreadsPerHost(maxBlockedThreadsPerHost)
                    .setMaxConnsPerHost(maxConnectionsPerHost)
                    .setConnectTimeout(connectionTimeout)
                    .setSeeds(seeds)
            )
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());
    }
}
