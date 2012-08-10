package org.atlasapi.persistence.lookup.cassandra;

import static org.atlasapi.persistence.cassandra.CassandraSchema.CLUSTER;
import static org.atlasapi.persistence.cassandra.CassandraSchema.KEYSPACE;
import static org.junit.Assert.assertEquals;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassandraLookupEntryStoreTest {
    
    private final AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder().forCluster(CLUSTER).forKeyspace(KEYSPACE).
            withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE)).
            withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(CLUSTER).setPort(9160).
            setMaxBlockedThreadsPerHost(5).
            setMaxConnsPerHost(5).
            setConnectTimeout(1000).
            setSeeds("localhost")).
            withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).
            buildKeyspace(ThriftFamilyFactory.getInstance());
    
    private final CassandraLookupEntryStore entryStore = new CassandraLookupEntryStore(context, 1000);
    
    @Before
    public void setup() {
        Logger.getRootLogger().setLevel(Level.INFO);
        Logger.getRootLogger().addAppender(new ConsoleAppender(new PatternLayout("%-6r [%p] %c - %m%n")));
        
        entryStore.init();
    }
    
    @After
    public void after() {
        entryStore.close();
    }
    
    @Test
    public void testStoreAndReadLookupEntry() {

        Item testItem = new Item("testItemUri", "testItemCurie", Publisher.BBC);
        testItem.addAlias("testItemAlias");
        
        LookupEntry testEntry = LookupEntry.lookupEntryFrom(testItem);
        entryStore.store(testEntry);
        
        Iterable<LookupEntry> uriEntry = entryStore.entriesForUris(ImmutableList.of("testItemUri"));
        assertEquals(testEntry, Iterables.getOnlyElement(uriEntry));
        
    }

}
