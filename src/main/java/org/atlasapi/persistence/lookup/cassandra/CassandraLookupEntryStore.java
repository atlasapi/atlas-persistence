package org.atlasapi.persistence.lookup.cassandra;

import static com.google.common.base.Predicates.notNull;
import static org.atlasapi.persistence.cassandra.CassandraSchema.CONTENT_CF;
import static org.atlasapi.persistence.cassandra.CassandraSchema.DFLT_EQUIV_COLUMN;
import static org.atlasapi.persistence.cassandra.CassandraSchema.EQUIV_COLUMN;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.cassandra.CassandraIndex;
import org.atlasapi.persistence.cassandra.CassandraPersistenceException;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.lookup.entry.LookupEntry;
import org.atlasapi.persistence.lookup.entry.LookupEntryStore;
import org.atlasapi.serialization.json.JsonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.concurrency.FutureList;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;

public class CassandraLookupEntryStore implements LookupEntryStore, NewLookupWriter {

    private static final Logger log = LoggerFactory.getLogger(CassandraLookupEntryStore.class);
    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    private final AstyanaxContext<Keyspace> context;
    private final CassandraIndex index;
    private final int requestTimeout;
    private final Keyspace keyspace;

    public CassandraLookupEntryStore(AstyanaxContext<Keyspace> context, int requestTimeout) {
        this.context = context;
        this.requestTimeout = requestTimeout;
        this.index = new CassandraIndex();
        this.keyspace = context.getEntity();
    }

    @Override
    public void store(LookupEntry entry) {
        try {
            MutationBatch mutation = marshalEntry(entry, EQUIV_COLUMN);
            Future<OperationResult<Void>> result = mutation.executeAsync();
            result.get(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    private MutationBatch marshalEntry(LookupEntry entry, String column) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch().setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        byte[] entryBytes = mapper.writer().writeValueAsBytes(entry);
        mutation.withRow(CONTENT_CF, entry.uri()).putColumn(column, entryBytes, null);
        return mutation;
    }

    @Override
    public Iterable<LookupEntry> entriesForCanonicalUris(Iterable<String> uris) {
        try {
            return entries(uris, CONTENT_CF);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    private Iterable<LookupEntry> entries(Iterable<String> uris, ColumnFamily<String, String> cf) throws ConnectionException, InterruptedException, ExecutionException, TimeoutException {
        Future<OperationResult<Rows<String, String>>> op = keyspace.prepareQuery(cf).setConsistencyLevel(ConsistencyLevel.CL_ONE).getKeySlice(uris).withColumnSlice(DFLT_EQUIV_COLUMN, EQUIV_COLUMN).executeAsync();
        Rows<String, String> rows = op.get(requestTimeout, TimeUnit.MILLISECONDS).getResult();
        return Iterables.filter(Iterables.transform(rows, ROW_TO_LOOKUP_ENTRY), notNull());
    }
    private final Function<Row<String, String>, LookupEntry> ROW_TO_LOOKUP_ENTRY = new Function<Row<String, String>, LookupEntry>() {

        @Override
        public LookupEntry apply(@Nullable Row<String, String> input) {
            try {
                return unmarshalEntry(input.getColumns());
            } catch (Exception e) {
                log.warn("Failed to unmarshall lookup entry", e);
                throw Throwables.propagate(e);
            }
        }
    };

    private LookupEntry unmarshalEntry(ColumnList<String> columns) throws Exception {
        Column<String> equivCol = columns.getColumnByName(EQUIV_COLUMN);
        if (equivCol == null) {
            equivCol = columns.getColumnByName(DFLT_EQUIV_COLUMN);
        }
        if (equivCol == null) {
            return null;
        }
        return mapper.readValue(equivCol.getByteArrayValue(), LookupEntry.class);
    }

    @Override
    public Iterable<LookupEntry> entriesForIds(Iterable<Long> ids) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ensureLookup(Content content) {
        try {
            LookupEntry entry = LookupEntry.lookupEntryFrom(content);
            MutationBatch mutation = marshalEntry(entry, DFLT_EQUIV_COLUMN);
            FutureList results = new FutureList();
            results.add(mutation.executeAsync());
            results.add(index.inverted(keyspace, CONTENT_CF, ConsistencyLevel.CL_QUORUM).
                    from(content.getCanonicalUri()).
                    index(content.getAllUris().iterator()).
                    execute());
            results.get(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Iterable<LookupEntry> entriesForIdentifiers(Iterable<String> identifiers) {
        return entriesForIds(identifiers, CONTENT_CF);
    }

    private Iterable<LookupEntry> entriesForIds(Iterable<String> identifiers, final ColumnFamily<String, String> cf) {
        final ConsistencyLevel cl = ConsistencyLevel.CL_ONE;
        return Iterables.concat(Iterables.transform(identifiers, new Function<String, Iterable<LookupEntry>>() {

            @Override
            public Iterable<LookupEntry> apply(@Nullable String input) {
                try {
                    Collection<String> ids = index.inverted(keyspace, cf, cl).lookup(input).execute(1, TimeUnit.MINUTES);
                    return entries(ids, cf);
                } catch (Exception e) {
                    throw new CassandraPersistenceException(e.getMessage(), e);
                }
            }
        }));
    }
}
