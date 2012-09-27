package org.atlasapi.persistence.cassandra;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.junit.After;
import org.junit.Before;
import static org.atlasapi.persistence.cassandra.CassandraSchema.*;
import org.junit.Ignore;

/**
 */
@Ignore
public class BaseCassandraTest {

    //
    //private static final String CASSANDRA_HOST = "cassandra1.owl.atlas.mbst.tv";
    private static final String CASSANDRA_HOST = "127.0.0.1";
    //
    protected AstyanaxContext context;
    //

    @Before
    public void before() {
        context = new AstyanaxContext.Builder().forCluster(CLUSTER).forKeyspace(getKeyspace("prod")).
                withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE)).
                withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(CLUSTER).setPort(9160).
                setMaxBlockedThreadsPerHost(Runtime.getRuntime().availableProcessors() * 10).
                setMaxConnsPerHost(Runtime.getRuntime().availableProcessors() * 10).
                setConnectTimeout(10000).
                setSeeds(CASSANDRA_HOST)).
                withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).
                buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
    }

    @After
    public void close() {
        context.shutdown();
    }
}
