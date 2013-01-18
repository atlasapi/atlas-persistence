package org.atlasapi.equiv;

import static org.atlasapi.persistence.cassandra.CassandraSchema.CLUSTER;
import static org.atlasapi.persistence.cassandra.CassandraSchema.getKeyspace;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.common.Id;
import org.atlasapi.persistence.cassandra.CassandraPersistenceException;
import org.atlasapi.serialization.json.JsonFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.query.IndexQuery;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/*
   update column family EquivalenceSummary with                            
   comparator = UTF8Type and
   column_metadata =
   [
     {column_name: summary, validation_class: UTF8Type},
     {column_name: parent, validation_class: UTF8Type, index_type: KEYS}
   ];
 */

public class CassandraEquivalenceSummaryStore implements EquivalenceSummaryStore {

    private static final String SUMMARY_CF_NAME = "EquivalenceSummary";
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
    
    @Deprecated
    public CassandraEquivalenceSummaryStore(String environment, ArrayList<String> seeds, int port, int maxConnections, int connectionTimeout, int requestTimeout) {
        this(createContext(environment, seeds, port, maxConnections, connectionTimeout), 
            requestTimeout
        );
    }

    protected static AstyanaxContext<Keyspace> createContext(String environment, ArrayList<String> seeds, int port, int maxConnections, int connectionTimeout) {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder().forCluster(CLUSTER).forKeyspace(getKeyspace(environment)).
                withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE)).
                withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(CLUSTER).setPort(port).
                setMaxBlockedThreadsPerHost(maxConnections).
                setMaxConnsPerHost(maxConnections).
                setConnectTimeout(connectionTimeout).
                setSeeds(Joiner.on(",").join(seeds))).
                withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).
                buildKeyspace(ThriftFamilyFactory.getInstance());
        context.start();
        return context;
    }

    @Override
    public void store(EquivalenceSummary summary) {
        try {
            MutationBatch mutationBatch = keyspace.prepareMutationBatch();
            mutationBatch.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
            ColumnListMutation<String> mutation = mutationBatch
                    .withRow(EQUIV_SUM_CF, summary.getSubject().toString())
                    .putColumn(SUMMARY_COL, serialize(summary), null);
            if (summary.getParent() != null) {
                mutation.putColumn(PARENT_COL, summary.getParent().toString(), null);
            }
            Future<OperationResult<Void>> result = mutationBatch.executeAsync();
            result.get(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new CassandraPersistenceException(summary.getSubject().toString(), e);
        }
    }

    private byte[] serialize(EquivalenceSummary summary) throws Exception {
        return mapper.writeValueAsBytes(summary);
    }

    @Override
    public Set<EquivalenceSummary> summariesForChildren(Id parent) {
        Rows<String, String> rows = rowsForParent(parent.toString());
        ImmutableSet.Builder<EquivalenceSummary> result = ImmutableSet.builder();
        for (Row<String, String> row : rows) {
            result.add(deserialize(row));
        }
        return result.build();
    }

    private Rows<String, String> rowsForParent(String parent) {
        try {
            IndexQuery<String, String> query = keyspace.prepareQuery(EQUIV_SUM_CF)
                    .searchWithIndex().addExpression()
                        .whereColumn(PARENT_COL).equals().value(parent);
            Future<OperationResult<Rows<String, String>>> queryResult = query.executeAsync();
            return queryResult.get(requestTimeout, TimeUnit.MILLISECONDS).getResult();
        } catch (Exception e) {
            throw new CassandraPersistenceException(parent, e);
        }
    }

    @Override
    public OptionalMap<Id, EquivalenceSummary> summariesForIds(Iterable<Id> ids) {
        return deserialize(ids, rowsForUris(ids));
    }

    private Rows<String, String> rowsForUris(Iterable<Id> ids) {
        try {
            ColumnFamilyQuery<String, String> query = keyspace
                    .prepareQuery(EQUIV_SUM_CF)
                    .setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
            RowSliceQuery<String, String> slice = query.getKeySlice(ImmutableSet.copyOf(Iterables.transform(ids,Functions.toStringFunction())));
            Future<OperationResult<Rows<String, String>>> queryResult = slice.executeAsync();
            return queryResult.get(requestTimeout, TimeUnit.MILLISECONDS).getResult();
        } catch (Exception e) {
            throw new CassandraPersistenceException(e.getMessage(), e);
        }
    }

    private OptionalMap<Id, EquivalenceSummary> deserialize(Iterable<Id> ids, Rows<String, String> result) {
        Builder<Id, Optional<EquivalenceSummary>> resultMap = ImmutableMap.builder();
        for (Id id : ids) {
            EquivalenceSummary value = deserialize(result.getRow(id.toString()));
            resultMap.put(id, Optional.fromNullable(value));
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
