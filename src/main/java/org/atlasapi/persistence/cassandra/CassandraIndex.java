package org.atlasapi.persistence.cassandra;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.query.RowQuery;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class CassandraIndex {

    public DirectIndex direct(Keyspace ks, ColumnFamily<String, String> cf, ConsistencyLevel cl) {
        return new DirectIndex(ks, cf, cl);
    }

    public InvertedIndex inverted(Keyspace ks, ColumnFamily<String, String> cf, ConsistencyLevel cl) {
        return new InvertedIndex(ks, cf, cl);
    }

    public static class DirectIndex {

        private final ColumnFamily<String, String> cf;
        private final MutationBatch mutation;
        private final ColumnFamilyQuery<String, String> lookup;
        private ColumnQuery<String> query;
        private String from;
        private String to;

        public DirectIndex(Keyspace ks, ColumnFamily<String, String> cf, ConsistencyLevel cl) {
            this.cf = cf;
            this.mutation = ks.prepareMutationBatch().setConsistencyLevel(cl);
            this.lookup = ks.prepareQuery(this.cf).setConsistencyLevel(cl);
        }

        public DirectIndex from(String from) {
            this.from = from;
            return this;
        }

        public DirectIndex to(String to) {
            this.to = to;
            return this;
        }

        public Index index() {
            if (from == null) {
                throw new IllegalStateException("Indexing requires you to call the 'from' method.");
            }
            if (to == null) {
                throw new IllegalStateException("Indexing requires you to call the 'to' method.");
            }
            mutation.withRow(cf, from).putColumn(cf.getName(), to);
            return new Index();
        }

        public Index delete() {
            if (from == null) {
                throw new IllegalStateException("Deleting requires you to call the 'from' method.");
            }
            mutation.deleteRow(Arrays.<ColumnFamily<String, ?>>asList(cf), from);
            return new Index();
        }

        public Lookup lookup() {
            if (from == null) {
                throw new IllegalStateException("Lookup requires you to call the 'from' method.");
            }
            this.query = lookup.getKey(from).getColumn(cf.getName());
            return new Lookup();
        }

        public class Index {

            public void async(long time, TimeUnit unit) throws Exception {
                mutation.executeAsync().get(time, unit);
            }
        }

        public class Lookup {

            public String async(long time, TimeUnit unit) throws Exception {
                try {
                    Column<String> result = query.executeAsync().get(time, unit).getResult();
                    if (result.hasValue()) {
                        return result.getStringValue();
                    } else {
                        return null;
                    }
                } catch (ExecutionException ex) {
                    if (ex.getCause() instanceof NotFoundException) {
                        return null;
                    } else {
                        throw ex;
                    }
                }
            }
        }
    }

    public static class InvertedIndex {

        private final ColumnFamily<String, String> cf;
        private final MutationBatch mutation;
        private final ColumnFamilyQuery<String, String> lookup;
        private RowQuery<String, String> query;
        private String source;

        public InvertedIndex(Keyspace ks, ColumnFamily<String, String> cf, ConsistencyLevel cl) {
            this.cf = cf;
            this.mutation = ks.prepareMutationBatch().setConsistencyLevel(cl);
            this.lookup = ks.prepareQuery(cf).setConsistencyLevel(cl);
        }

        public InvertedIndex from(String source) {
            this.source = source;
            return this;
        }

        public Index index(String... values) {
            if (source == null) {
                throw new IllegalStateException("Indexing requires you to call the 'from' method.");
            }
            for (String value : values) {
                mutation.withRow(cf, value).putEmptyColumn(source);
            }
            return new Index();
        }

        public Index delete(String... values) {
            if (source == null) {
                throw new IllegalStateException("Indexing requires you to call the 'from' method.");
            }
            for (String value : values) {
                mutation.withRow(cf, value).deleteColumn(source);
            }
            return new Index();
        }

        public Lookup lookup(String value) {
            this.query = lookup.getKey(value);
            return new Lookup();
        }

        public class Index {

            public void async(long time, TimeUnit unit) throws Exception {
                mutation.executeAsync().get(time, unit);
            }
        }

        public class Lookup {

            public Collection<String> async(long time, TimeUnit unit) throws Exception {
                try {
                    ColumnList<String> result = query.executeAsync().get(time, unit).getResult();
                    if (!result.isEmpty()) {
                        return result.getColumnNames();
                    } else {
                        return Collections.EMPTY_LIST;
                    }
                } catch (ExecutionException ex) {
                    if (ex.getCause() instanceof NotFoundException) {
                        return null;
                    } else {
                        throw ex;
                    }
                }
            }
        }
    }
}
