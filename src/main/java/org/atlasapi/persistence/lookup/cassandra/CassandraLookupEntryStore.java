package org.atlasapi.persistence.lookup.cassandra;

import static com.google.common.base.Predicates.notNull;
import static org.atlasapi.persistence.cassandra.CassandraSchema.CONTAINER_CF;
import static org.atlasapi.persistence.cassandra.CassandraSchema.DFLT_EQUIV_COLUMN;
import static org.atlasapi.persistence.cassandra.CassandraSchema.EQUIV_COLUMN;
import static org.atlasapi.persistence.cassandra.CassandraSchema.ITEMS_CF;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.cassandra.CassandraPersistenceException;
import org.atlasapi.persistence.content.ContentCategory;
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
    
    private final AstyanaxContext<Keyspace> context;
    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    private final int requestTimeout;

    private Keyspace keyspace;

    public CassandraLookupEntryStore(AstyanaxContext<Keyspace> context, int requestTimeout) {
        this.context = context;
        this.requestTimeout = requestTimeout;
    }
    
    @PostConstruct
    public void init() {
        context.start();
        keyspace = context.getEntity();
    }
    
    @PreDestroy
    public void close() {
        context.shutdown();
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
        MutationBatch mutation = keyspace.prepareMutationBatch()
                .setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        ColumnFamily<String, String> cf = columnFamily(entry);
        byte[] entryBytes = mapper.writer().writeValueAsBytes(entry);
        mutation.withRow(cf, entry.uri()).putColumn(column, entryBytes, null);
        return mutation;
    }

    protected ColumnFamily<String, String> columnFamily(LookupEntry entry) {
        ColumnFamily<String, String> cf;
        if (ContentCategory.CONTAINERS.contains(entry.lookupRef().category())) {
            cf = CONTAINER_CF;
        } else {
            cf = ITEMS_CF;
        }
        return cf;
    }

    @Override
    public Iterable<LookupEntry> entriesForUris(Iterable<String> uris) {
        try {
            return Iterables.concat(
                entries(uris, ITEMS_CF),entries(uris, CONTAINER_CF)
            );
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    private Iterable<LookupEntry> entries(Iterable<String> uris, ColumnFamily<String, String> cf) throws ConnectionException, InterruptedException, ExecutionException, TimeoutException {
        Future<OperationResult<Rows<String, String>>> op = keyspace.prepareQuery(cf)
                .setConsistencyLevel(ConsistencyLevel.CL_QUORUM)
                .getKeySlice(ImmutableList.copyOf(uris))
                .withColumnSlice(DFLT_EQUIV_COLUMN, EQUIV_COLUMN)
                .executeAsync();

        Rows<String, String> rows = op.get(requestTimeout, TimeUnit.MILLISECONDS).getResult();
        return Iterables.filter(Iterables.transform(rows, ROW_TO_LOOKUP_ENTRY), notNull());
    }
    
    private final Function<Row<String,String>,LookupEntry> ROW_TO_LOOKUP_ENTRY = new Function<Row<String,String>, LookupEntry>(){

        @Override
        public LookupEntry apply(@Nullable Row<String,String> input) {
            try {
                return unmarshalEntry(input.getColumns());
            } catch (Exception e) {
                log.warn("Failed to unmarshall lookup entry", e);
                throw Throwables.propagate(e);
            }
        }}; 

        
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
            Future<OperationResult<Void>> result = mutation.executeAsync();
            result.get(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

}
