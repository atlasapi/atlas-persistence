package org.atlasapi.persistence;

import com.metabroadcast.common.ids.IdGenerator;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.atlasapi.persistence.content.cassandra.CassandraContentStore;
import org.atlasapi.persistence.content.cassandra.CassandraContentGroupStore;
import org.atlasapi.persistence.content.cassandra.CassandraProductStore;
import org.atlasapi.persistence.content.people.cassandra.CassandraPersonStore;
import org.atlasapi.persistence.media.channel.cassandra.CassandraChannelGroupStore;
import org.atlasapi.persistence.media.channel.cassandra.CassandraChannelStore;
import org.atlasapi.persistence.media.segment.cassandra.CassandraSegmentStore;
import org.atlasapi.persistence.topic.cassandra.CassandraTopicStore;
import org.joda.time.Duration;
import static org.atlasapi.persistence.cassandra.CassandraSchema.*;

/**
 */
public class CassandraContentPersistenceModule {

    private final AstyanaxContext<Keyspace> cassandraContext;
    //
    private final CassandraContentStore cassandraContentStore;
    private final CassandraChannelGroupStore cassandraChannelGroupStore;
    private final CassandraChannelStore cassandraChannelStore;
    private final CassandraContentGroupStore cassandraContentGroupStore;
    private final CassandraPersonStore cassandraPersonStore;
    private final CassandraProductStore cassandraProductStore;
    private final CassandraSegmentStore cassandraSegmentStore;
    private final CassandraTopicStore cassandraTopicStore;

    public CassandraContentPersistenceModule(String environment, String seeds, int port, int connectionTimeout, int requestTimeout, IdGenerator idGenerator) {
        this.cassandraContext = new AstyanaxContext.Builder().forCluster(CLUSTER).forKeyspace(getKeyspace(environment)).
                withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE)).
                withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(CLUSTER).setPort(port).
                setMaxBlockedThreadsPerHost(Runtime.getRuntime().availableProcessors() * 10).
                setMaxConnsPerHost(Runtime.getRuntime().availableProcessors() * 10).
                setConnectTimeout(connectionTimeout).
                setSeeds(seeds)).
                withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).
                buildKeyspace(ThriftFamilyFactory.getInstance());
        //
        this.cassandraContext.start();
        //
        this.cassandraContentStore = new CassandraContentStore(cassandraContext, requestTimeout);
        this.cassandraChannelGroupStore = new CassandraChannelGroupStore(cassandraContext, requestTimeout);
        this.cassandraChannelStore = new CassandraChannelStore(cassandraContext, requestTimeout, idGenerator, Duration.standardMinutes(15));
        this.cassandraContentGroupStore = new CassandraContentGroupStore(cassandraContext, requestTimeout);
        this.cassandraPersonStore = new CassandraPersonStore(cassandraContext, requestTimeout);
        this.cassandraProductStore = new CassandraProductStore(cassandraContext, requestTimeout);
        this.cassandraSegmentStore = new CassandraSegmentStore(cassandraContext, requestTimeout);
        this.cassandraTopicStore = new CassandraTopicStore(cassandraContext, requestTimeout);
    }

    public void destroy() {
        cassandraContext.shutdown();
    }

    public CassandraContentStore cassandraContentStore() {
        return cassandraContentStore;
    }

    public CassandraChannelGroupStore cassandraChannelGroupStore() {
        return cassandraChannelGroupStore;
    }

    public CassandraChannelStore cassandraChannelStore() {
        return cassandraChannelStore;
    }

    public CassandraContentGroupStore cassandraContentGroupStore() {
        return cassandraContentGroupStore;
    }

    public CassandraPersonStore cassandraPersonStore() {
        return cassandraPersonStore;
    }

    public CassandraProductStore cassandraProductStore() {
        return cassandraProductStore;
    }

    public CassandraSegmentStore cassandraSegmentStore() {
        return cassandraSegmentStore;
    }

    public CassandraTopicStore cassandraTopicStore() {
        return cassandraTopicStore;
    }
}
