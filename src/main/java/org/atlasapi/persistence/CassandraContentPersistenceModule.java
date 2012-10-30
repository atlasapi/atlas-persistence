package org.atlasapi.persistence;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metabroadcast.common.ids.IdGenerator;

import org.atlasapi.equiv.CassandraEquivalenceSummaryStore;
import org.atlasapi.persistence.lookup.cassandra.CassandraLookupEntryStore;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import java.util.concurrent.Executors;
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
    private final CassandraLookupEntryStore cassandraLookupEntryStore;
    private final CassandraEquivalenceSummaryStore cassandraEquivalenceSummaryStore;
    private final CassandraContentStore cassandraContentStore;
    private final CassandraChannelGroupStore cassandraChannelGroupStore;
    private final CassandraChannelStore cassandraChannelStore;
    private final CassandraContentGroupStore cassandraContentGroupStore;
    private final CassandraPersonStore cassandraPersonStore;
    private final CassandraProductStore cassandraProductStore;
    private final CassandraSegmentStore cassandraSegmentStore;
    private final CassandraTopicStore cassandraTopicStore;

    public CassandraContentPersistenceModule(String environment, String seeds, int port, int connectionTimeout, int requestTimeout, int clientThreads, IdGenerator idGenerator) {
        this.cassandraContext = new AstyanaxContext.Builder().forCluster(CLUSTER).forKeyspace(getKeyspace(environment)).
                withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE).
                setConnectionPoolType(ConnectionPoolType.TOKEN_AWARE).
                setAsyncExecutor(Executors.newFixedThreadPool(clientThreads, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("AstyanaxAsync-%d").build()))).
                withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(CLUSTER).setPort(port).
                setMaxBlockedThreadsPerHost(clientThreads).
                setMaxConnsPerHost(clientThreads).
                setMaxConns(clientThreads * 5).
                setConnectTimeout(connectionTimeout).
                setSeeds(seeds)).
                withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).
                buildKeyspace(ThriftFamilyFactory.getInstance());
        //
        this.cassandraContext.start();
        //
        this.cassandraLookupEntryStore = new CassandraLookupEntryStore(cassandraContext, requestTimeout);
        this.cassandraEquivalenceSummaryStore = new CassandraEquivalenceSummaryStore(cassandraContext, requestTimeout);
        this.cassandraContentStore = new CassandraContentStore(cassandraContext, requestTimeout, cassandraLookupEntryStore);
        this.cassandraChannelGroupStore = new CassandraChannelGroupStore(cassandraContext, requestTimeout);
        this.cassandraChannelStore = new CassandraChannelStore(cassandraContext, requestTimeout, idGenerator, Duration.standardMinutes(15));
        this.cassandraContentGroupStore = new CassandraContentGroupStore(cassandraContext, requestTimeout);
        this.cassandraPersonStore = new CassandraPersonStore(cassandraContext, requestTimeout);
        this.cassandraProductStore = new CassandraProductStore(cassandraContext, requestTimeout, idGenerator);
        this.cassandraSegmentStore = new CassandraSegmentStore(cassandraContext, requestTimeout, idGenerator);
        this.cassandraTopicStore = new CassandraTopicStore(cassandraContext, requestTimeout);
    }

    public void destroy() {
        cassandraContext.shutdown();
    }

    public CassandraLookupEntryStore cassandraLookupEntryStore() {
        return cassandraLookupEntryStore;
    }

    public CassandraEquivalenceSummaryStore cassandraEquivalenceSummaryStore() {
        return cassandraEquivalenceSummaryStore;
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
