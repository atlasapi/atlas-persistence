package org.atlasapi.equiv;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.cassandra.CassandraPersistenceException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.reflect.TypeToken;
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

    private final class PublisherAdapter implements JsonSerializer<Publisher>, JsonDeserializer<Publisher> {

        @Override
        public JsonElement serialize(Publisher src, Type typeOfSrc, JsonSerializationContext context) {
            return new JsonPrimitive(src.key());
        }
        
        @Override
        public Publisher deserialize(JsonElement json, Type typeOfT,
                JsonDeserializationContext context) throws JsonParseException {
            return Publisher.fromKey(json.getAsString()).requireValue();
        }
        
    }

    private static final class EquivalenceSummaryDeserializer implements
            JsonDeserializer<EquivalenceSummary> {

        private static final TypeToken<Map<Publisher,ContentRef>> equivsTypeToken
            = new TypeToken<Map<Publisher,ContentRef>>(){};
        private static final TypeToken<List<String>> candidatesTypeToken
            = new TypeToken<List<String>>(){};

        @Override
        public EquivalenceSummary deserialize(JsonElement json, Type typeOfT,
                JsonDeserializationContext context) throws JsonParseException {
            JsonObject obj = json.getAsJsonObject();
            String subject = obj.get("subject").getAsString();
            String parent = deserializeParent(obj.get("parent"));
            List<String> candidates = deserializeCandidates(context, obj.get("candidates"));
            Map<Publisher,ContentRef> equivalents = deserializeEquivalents(context, obj.get("equivalents"));
            return new EquivalenceSummary(subject, parent, candidates, equivalents);
        }

        private String deserializeParent(JsonElement parent) {
            return parent == null ? null : parent.getAsString();
        }

        private Map<Publisher, ContentRef> deserializeEquivalents(
                JsonDeserializationContext context, JsonElement equivs) {
            return context.deserialize(equivs, equivsTypeToken.getType());
        }

        private List<String> deserializeCandidates(JsonDeserializationContext context,
                JsonElement candidates) {
            return context.deserialize(candidates, candidatesTypeToken.getType());
        }
    }

    private static final String SUMMARY_CF_NAME = "EquivalenceSummaries";
    private static final String SUMMARY_COL = "summary";
    private static final String PARENT_COL = "parent";
    static final ColumnFamily<String, String> EQUIV_SUM_CF = 
            new ColumnFamily<String, String>(
                    SUMMARY_CF_NAME, 
                    StringSerializer.get(), 
                    StringSerializer.get()
            );
    
    private final Gson gson = new GsonBuilder()
        .registerTypeAdapter(EquivalenceSummary.class, new EquivalenceSummaryDeserializer())
        .registerTypeAdapter(Publisher.class, new PublisherAdapter())
        .create();

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
        return gson.toJson(summary).getBytes();
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
            InputStreamReader reader = new InputStreamReader(new ByteArrayInputStream(columnBytes));
            return gson.fromJson(reader, EquivalenceSummary.class);
        } catch (Exception e) {
            throw new CassandraPersistenceException(row.getKey(), e);
        }
    }

}
