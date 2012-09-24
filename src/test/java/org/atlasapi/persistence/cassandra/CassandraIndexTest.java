package org.atlasapi.persistence.cassandra;

import com.google.common.collect.Iterables;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.StringSerializer;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 */
//@Ignore(value = "Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraIndexTest extends BaseCassandraTest {

    @Test
    public void testDirectIndex() throws Exception {
        Keyspace ks = (Keyspace) context.getEntity();
        ColumnFamily<String, String> cf = new ColumnFamily<String, String>(
                "TestIndex",
                StringSerializer.get(),
                StringSerializer.get());
        ConsistencyLevel cl = ConsistencyLevel.CL_QUORUM;
        CassandraIndex index = new CassandraIndex();

        index.direct(ks, cf, cl).from("a").to("b").index().async(1, TimeUnit.MINUTES);
        assertEquals("b", index.direct(ks, cf, cl).from("a").lookup().async(1, TimeUnit.MINUTES));
        
        index.direct(ks, cf, cl).from("a").delete().async(1, TimeUnit.MINUTES);
        assertEquals(null, index.direct(ks, cf, cl).from("a").lookup().async(1, TimeUnit.MINUTES));
    }
    
    @Test
    public void testInvertedIndex() throws Exception {
        Keyspace ks = (Keyspace) context.getEntity();
        ColumnFamily<String, String> cf = new ColumnFamily<String, String>(
                "TestIndex",
                StringSerializer.get(),
                StringSerializer.get());
        ConsistencyLevel cl = ConsistencyLevel.CL_QUORUM;
        CassandraIndex index = new CassandraIndex();

        index.inverted(ks, cf, cl).from("a").index("v1","v2").async(1, TimeUnit.MINUTES);
        index.inverted(ks, cf, cl).from("b").index("v1").async(1, TimeUnit.MINUTES);
        assertEquals(2, index.inverted(ks, cf, cl).lookup("v1").async(1, TimeUnit.MINUTES).size());
        
        index.inverted(ks, cf, cl).from("b").delete("v1").async(1, TimeUnit.MINUTES);
        assertEquals(1, index.inverted(ks, cf, cl).lookup("v1").async(1, TimeUnit.MINUTES).size());
        assertEquals("a", Iterables.get(index.inverted(ks, cf, cl).lookup("v1").async(1, TimeUnit.MINUTES), 0));
    }
}
