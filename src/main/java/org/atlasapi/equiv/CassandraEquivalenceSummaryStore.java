package org.atlasapi.equiv;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.atlasapi.persistence.cassandra.CassandraPersistenceException;
import org.atlasapi.serialization.json.JsonFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.StringSerializer;

public class CassandraEquivalenceSummaryStore implements EquivalenceSummaryStore {

    private static final String SUMMARY_CF_NAME = "EquivalenceSummaries";
    private static final String SUMMARY_COL = "summary";
    private static final String PARENT_COL = "parent";
    static final ColumnFamily<String, String> EQUIV_SUM_CF = 
            new ColumnFamily<String, String>(
                    SUMMARY_CF_NAME, 
                    StringSerializer.get(), 
                    StringSerializer.get()
            );

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    
    private final Keyspace keyspace;
    private final int requestTimeout;
    
    public CassandraEquivalenceSummaryStore(AstyanaxContext<Keyspace> context, int requestTimeout) {
        this.keyspace = context.getEntity();
        this.requestTimeout = requestTimeout;
    }

    @Override
    public void store(EquivalenceSummary summary) {
        try {
            MutationBatch mutationBatch = keyspace.prepareMutationBatch();
            mutationBatch.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
            ColumnListMutation<String> mutation = mutationBatch
                    .withRow(EQUIV_SUM_CF, summary.getSubject())
                    .putColumn(SUMMARY_COL, serialize(summary), null);
            if (summary.getParent() != null) {
                mutation.putColumn(PARENT_COL, summary.getParent(), null);
            }
            Future<OperationResult<Void>> result = mutationBatch.executeAsync();
            result.get(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new CassandraPersistenceException(summary.getSubject(), e);
        }
    }

    private byte[] serialize(EquivalenceSummary summary) throws Exception {
        return mapper.writeValueAsBytes(summary);
    }

    @Override
    public OptionalMap<String, EquivalenceSummary> summariesForUris(Iterable<String> uris) {
        return deserialize(uris, rowsForUris(uris));
    }

    private Rows<String, String> rowsForUris(Iterable<String> uris) {
        try {
            ColumnFamilyQuery<String, String> query = keyspace
                    .prepareQuery(EQUIV_SUM_CF)
                    .setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
            RowSliceQuery<String, String> slice = query.getKeySlice(ImmutableSet.copyOf(uris));
            Future<OperationResult<Rows<String, String>>> queryResult = slice.executeAsync();
            return queryResult.get(requestTimeout, TimeUnit.MILLISECONDS).getResult();
        } catch (Exception e) {
            throw new CassandraPersistenceException(e.getMessage(), e);
        }
    }

    private OptionalMap<String, EquivalenceSummary> deserialize(Iterable<String> uris, Rows<String, String> result) {
        Builder<String, Optional<EquivalenceSummary>> resultMap = ImmutableMap.builder();
        for (String uri : uris) {
            EquivalenceSummary value = deserialize(result.getRow(uri));
            resultMap.put(uri, Optional.fromNullable(value));
        }
        return ImmutableOptionalMap.copyOf(resultMap.build());
    }


    private EquivalenceSummary deserialize(Row<String, String> row) {
        try {
            Column<String> column = row.getColumns().getColumnByName(SUMMARY_COL);
            if (column == null) {
                return null;
            }
            byte[] columnBytes = column.getByteArrayValue();
            return mapper.readValue(columnBytes, EquivalenceSummary.class);
        } catch (Exception e) {
            throw new CassandraPersistenceException(row.getKey(), e);
        }
    }

}
