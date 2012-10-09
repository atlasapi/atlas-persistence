package org.atlasapi.persistence.cassandra;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnMap;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.retry.RetryPolicy;
import com.netflix.astyanax.retry.SleepingRetryPolicy;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.StringUtils;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 */
public class CassandraId {

    private final AstyanaxContext<Keyspace> context;
    private final Keyspace keyspace;
    private final ColumnFamily<String, String> lockColumnFamily;
    private final String targetColumnFamily;
    private final long stripeId;
    private final long firstId;
    private final long idIncrement;
    private final long batchLength;
    private int lockTimeout = 60000;
    private int retryTimeout = 10000;
    private IdBatch batch = null;

    public static CassandraId plainGenerator(AstyanaxContext<Keyspace> context, String lockColumnFamily, String targetColumnFamily, long firstId, long batchLength) {
        return new CassandraId(context, lockColumnFamily, targetColumnFamily, -1, firstId, 1, batchLength);
    }

    public static CassandraId stripedGenerator(AstyanaxContext<Keyspace> context, String lockColumnFamily, String targetColumnFamily, long stripeId, long stripes, long batchLength) {
        return new CassandraId(context, lockColumnFamily, targetColumnFamily, stripeId, stripeId, stripes, batchLength);
    }

    private CassandraId(AstyanaxContext<Keyspace> context, String lockColumnFamily, String targetColumnFamily, long stripeId, long firstId, long idIncrement, long batchLength) {
        if (stripeId < -1) {
            throw new IllegalArgumentException("Stripe id must be greater than or equal to -1!");
        }
        if (firstId < 0) {
            throw new IllegalArgumentException("First id must be greater than or equal to 0!");
        }
        if (idIncrement < 1) {
            throw new IllegalArgumentException("Id increment must be greater than or equal to 1!");
        }
        if (batchLength < 1) {
            throw new IllegalArgumentException("Batch length must be greater than or equal to 1!");
        }
        this.context = context;
        this.lockColumnFamily = new ColumnFamily<String, String>(lockColumnFamily, StringSerializer.get(), StringSerializer.get());
        this.targetColumnFamily = targetColumnFamily;
        this.keyspace = context.getEntity();
        this.stripeId = stripeId;
        this.firstId = firstId;
        this.idIncrement = idIncrement;
        this.batchLength = batchLength;
    }

    public CassandraId withLockTimeout(int lockTimeout) {
        if (lockTimeout < 0) {
            throw new IllegalArgumentException("Lock timeout must be greater than or equal to 0 (milliseconds)!");
        }
        this.lockTimeout = lockTimeout;
        return this;
    }

    public CassandraId withRetryTimeout(int retryTimeout) {
        if (retryTimeout < 1000) {
            throw new IllegalArgumentException("Retry timeout must be greater than or equal to 1000 (milliseconds)!");
        }
        this.retryTimeout = retryTimeout;
        return this;
    }

    public synchronized long generate() {
        if (batch == null || !batch.hasNext()) {
            ColumnPrefixDistributedRowLock<String> lock = new ColumnPrefixDistributedRowLock<String>(keyspace, lockColumnFamily, rowLockKey()).withConsistencyLevel(ConsistencyLevel.CL_QUORUM).
                    withDataColumns(true).
                    withBackoff(new BoundedRandomBackoff(100, retryTimeout)).
                    expireLockAfter(lockTimeout, TimeUnit.MILLISECONDS);
            try {
                ColumnMap<String> row = lock.acquireLockAndReadRow();
                Long currentBatch = row.getLong(targetColumnFamily, firstId);
                Long nextBatch = currentBatch + (idIncrement * batchLength);
                MutationBatch update = keyspace.prepareMutationBatch().setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
                update.withRow(lockColumnFamily, rowLockKey()).putColumn(targetColumnFamily, nextBatch);
                batch = new IdBatch(currentBatch, idIncrement, nextBatch);
                lock.releaseWithMutation(update);
            } catch (Exception ex) {
                throw new CassandraPersistenceException(ex.getMessage(), ex);
            } finally {
                try {
                    lock.release();
                } catch (Exception ex) {
                    throw new CassandraPersistenceException(ex.getMessage(), ex);
                }
            }
        }
        return batch.next();
    }

    private String rowLockKey() {
        if (stripeId == -1) {
            return targetColumnFamily;
        } else {
            return new StringBuilder(targetColumnFamily).append("-").append(stripeId).toString();
        }
    }

    private static class IdBatch {

        private final long increment;
        private final long max;
        public long current;

        public IdBatch(long start, long increment, long max) {
            this.current = start;
            this.increment = increment;
            this.max = max;
        }

        public boolean hasNext() {
            return current < max;
        }

        public Long next() {
            if (current == max) {
                return null;
            } else {
                Long next = current;
                current += increment;
                return next;
            }
        }
    }

    public class BoundedRandomBackoff extends SleepingRetryPolicy {

        private final Random backoffGenerator = new Random(Thread.currentThread().getId());
        private final int baseSleepTimeMs;
        private final int maxSleepTimeMs;

        public BoundedRandomBackoff(int baseSleepTimeMs, int maxSleepTimeMs) {
            super(Integer.MAX_VALUE);
            this.baseSleepTimeMs = backoffGenerator.nextInt(baseSleepTimeMs) + 1;
            this.maxSleepTimeMs = maxSleepTimeMs;
        }

        @Override
        public boolean allowRetry() {
            if (getSleepTimeMs() > maxSleepTimeMs) {
                return false;
            } else {
                return super.allowRetry();
            }
        }

        @Override
        public long getSleepTimeMs() {
            return baseSleepTimeMs * (getAttemptCount() + 1);
        }

        @Override
        public RetryPolicy duplicate() {
            return new BoundedRandomBackoff(this.baseSleepTimeMs, this.maxSleepTimeMs);
        }

        @Override
        public String toString() {
            return StringUtils.joinClassAttributeValues(this, "BoundedRandomBackoff", BoundedRandomBackoff.class);
        }
    }
}
