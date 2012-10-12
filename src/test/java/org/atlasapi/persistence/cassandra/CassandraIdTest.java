package org.atlasapi.persistence.cassandra;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 */
//@Ignore(value = "Enable if running a local Cassandra instance with proper schema.")
// TODO: some duplicated code, refactor later.
public class CassandraIdTest extends BaseCassandraTest {

    @Test
    public void testIdGeneration() throws Exception {
        ColumnFamily<String, String> lockCF = new ColumnFamily<String, String>(
                "Lock",
                StringSerializer.get(),
                StringSerializer.get());
        ColumnFamily<String, String> targetCF = new ColumnFamily<String, String>(
                "Target",
                StringSerializer.get(),
                StringSerializer.get());
        try {
            CassandraId cassandraId = CassandraId.plainGenerator(context, lockCF.getName(), targetCF.getName(), 0, 1);
            //
            assertEquals(0, cassandraId.generate());
            assertCassandraLockValue(lockCF, targetCF, 1l);
            assertEquals(1, cassandraId.generate());
            assertCassandraLockValue(lockCF, targetCF, 2l);
            assertEquals(2, cassandraId.generate());
            assertCassandraLockValue(lockCF, targetCF, 3l);
        } finally {
            removeLocks(lockCF, targetCF);
        }
    }

    @Test
    public void testBatchedIdGeneration() throws Exception {
        ColumnFamily<String, String> lockCF = new ColumnFamily<String, String>(
                "Lock",
                StringSerializer.get(),
                StringSerializer.get());
        ColumnFamily<String, String> targetCF = new ColumnFamily<String, String>(
                "Target",
                StringSerializer.get(),
                StringSerializer.get());
        try {
            CassandraId cassandraId = CassandraId.plainGenerator(context, lockCF.getName(), targetCF.getName(), 0, 2);
            //
            assertEquals(0, cassandraId.generate());
            assertCassandraLockValue(lockCF, targetCF, 2l);
            assertEquals(1, cassandraId.generate());
            assertCassandraLockValue(lockCF, targetCF, 2l);
            assertEquals(2, cassandraId.generate());
            assertCassandraLockValue(lockCF, targetCF, 4l);
        } finally {
            removeLocks(lockCF, targetCF);
        }
    }

    @Test
    public void testStripedIdGeneration() throws Exception {
        ColumnFamily<String, String> lockCF = new ColumnFamily<String, String>(
                "Lock",
                StringSerializer.get(),
                StringSerializer.get());
        ColumnFamily<String, String> targetCF = new ColumnFamily<String, String>(
                "Target",
                StringSerializer.get(),
                StringSerializer.get());
        try {
            CassandraId cassandraId1 = CassandraId.stripedGenerator(context, lockCF.getName(), targetCF.getName(), 0, 2, 1);
            CassandraId cassandraId2 = CassandraId.stripedGenerator(context, lockCF.getName(), targetCF.getName(), 1, 2, 1);
            //
            assertEquals(0, cassandraId1.generate());
            assertEquals(1, cassandraId2.generate());
            assertEquals(2, cassandraId1.generate());
            assertEquals(3, cassandraId2.generate());
            assertEquals(4, cassandraId1.generate());
            assertEquals(5, cassandraId2.generate());
        } finally {
            removeLocks(lockCF, targetCF, 0);
            removeLocks(lockCF, targetCF, 1);
        }
    }

    @Test
    public void testMultiThreadIdGeneration() throws Exception {
        ColumnFamily<String, String> lockCF = new ColumnFamily<String, String>(
                "Lock",
                StringSerializer.get(),
                StringSerializer.get());
        ColumnFamily<String, String> targetCF = new ColumnFamily<String, String>(
                "Target",
                StringSerializer.get(),
                StringSerializer.get());

        try {
            int times = 1000;
            //
            ExecutorService executor = Executors.newFixedThreadPool(10);
            //
            final CassandraId cassandraId = CassandraId.plainGenerator(context, lockCF.getName(), targetCF.getName(), 0, 10);
            final ConcurrentMap<Long, Long> ids = new ConcurrentHashMap<Long, Long>();
            final CountDownLatch generated = new CountDownLatch(times);
            for (int i = 0; i < times; i++) {
                executor.submit(new Runnable() {

                    @Override
                    public void run() {
                        Long id = cassandraId.generate();
                        if (ids.putIfAbsent(id, id) == null) {
                            generated.countDown();
                        }
                    }
                });
            }
            assertTrue(generated.await(10, TimeUnit.SECONDS));
        } finally {
            removeLocks(lockCF, targetCF);
        }
    }

    @Test
    public void testMultiInstanceIdGeneration() throws Exception {
        final ColumnFamily<String, String> lockCF = new ColumnFamily<String, String>(
                "Lock",
                StringSerializer.get(),
                StringSerializer.get());
        final ColumnFamily<String, String> targetCF = new ColumnFamily<String, String>(
                "Target",
                StringSerializer.get(),
                StringSerializer.get());

        try {
            int times = 100000;
            //
            ExecutorService executor = Executors.newFixedThreadPool(10);
            //
            final ThreadLocal<CassandraId> cassandraId = new ThreadLocal<CassandraId>() {

                @Override
                protected CassandraId initialValue() {
                    return CassandraId.plainGenerator(context, lockCF.getName(), targetCF.getName(), 0, 100);
                }
            };
            final ConcurrentMap<Long, Long> ids = new ConcurrentHashMap<Long, Long>();
            final CountDownLatch generated = new CountDownLatch(times);
            for (int i = 0; i < times; i++) {
                executor.submit(new Runnable() {

                    @Override
                    public void run() {
                        Long id = cassandraId.get().generate();
                        if (ids.putIfAbsent(id, id) == null) {
                            generated.countDown();
                        }
                    }
                });
            }
            assertTrue(generated.await(10, TimeUnit.SECONDS));
        } finally {
            removeLocks(lockCF, targetCF);
        }
    }

    @Test
    public void testStripedMultiInstanceIdGeneration() throws Exception {
        final ColumnFamily<String, String> lockCF = new ColumnFamily<String, String>(
                "Lock",
                StringSerializer.get(),
                StringSerializer.get());
        final ColumnFamily<String, String> targetCF = new ColumnFamily<String, String>(
                "Target",
                StringSerializer.get(),
                StringSerializer.get());

        try {
            int times = 100000;
            //
            ExecutorService executor = Executors.newFixedThreadPool(10);
            //
            final ThreadLocal<CassandraId> cassandraId = new ThreadLocal<CassandraId>() {

                @Override
                protected CassandraId initialValue() {
                    int stripe = (int) Thread.currentThread().getId() % 5;
                    return CassandraId.stripedGenerator(context, lockCF.getName(), targetCF.getName(), stripe, 5, 100);
                }
            };
            final ConcurrentMap<Long, Long> ids = new ConcurrentHashMap<Long, Long>();
            final CountDownLatch generated = new CountDownLatch(times);
            for (int i = 0; i < times; i++) {
                executor.submit(new Runnable() {

                    @Override
                    public void run() {
                        Long id = cassandraId.get().generate();
                        if (ids.putIfAbsent(id, id) == null) {
                            generated.countDown();
                        }
                    }
                });
            }
            assertTrue(generated.await(10, TimeUnit.SECONDS));
        } finally {
            removeLocks(lockCF, targetCF, 0);
            removeLocks(lockCF, targetCF, 1);
        }
    }

    private void assertCassandraLockValue(ColumnFamily<String, String> lockCF, ColumnFamily<String, String> targetCF, Long expected) throws Exception {
        Keyspace ks = (Keyspace) context.getEntity();
        Long actual = ks.prepareQuery(lockCF).getKey(targetCF.getName()).getColumn(targetCF.getName()).execute().getResult().getLongValue();
        assertEquals(expected, actual);
    }

    private void removeLocks(ColumnFamily<String, String> lockCF, ColumnFamily<String, String> targetCF) throws Exception {
        Keyspace ks = (Keyspace) context.getEntity();
        MutationBatch mutation = ks.prepareMutationBatch();
        mutation.deleteRow(Arrays.<ColumnFamily<String, ?>>asList(lockCF), targetCF.getName());
        mutation.execute().getResult();
    }

    private void removeLocks(ColumnFamily<String, String> lockCF, ColumnFamily<String, String> targetCF, int stripe) throws Exception {
        Keyspace ks = (Keyspace) context.getEntity();
        MutationBatch mutation = ks.prepareMutationBatch();
        mutation.deleteRow(Arrays.<ColumnFamily<String, ?>>asList(lockCF), targetCF.getName() + "-" + stripe);
        mutation.execute().getResult();
    }
}
