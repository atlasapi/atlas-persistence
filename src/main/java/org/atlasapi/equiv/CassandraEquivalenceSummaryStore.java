package org.atlasapi.equiv;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.cassandra.CassandraPersistenceException;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
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

        private static final TypeToken<PersistentEquivalencesContentRefs> equivalentContentRefsTypeToken = new TypeToken<PersistentEquivalencesContentRefs>(){};
        private static final TypeToken<List<String>> candidatesTypeToken
            = new TypeToken<List<String>>(){};

        @Override
        public EquivalenceSummary deserialize(JsonElement json, Type typeOfT,
                JsonDeserializationContext context) throws JsonParseException {
            JsonObject obj = json.getAsJsonObject();
            String subject = obj.get("subject").getAsString();
            String parent = deserializeParent(obj.get("parent"));
            List<String> candidates = deserializeCandidates(context, obj.get("candidates"));
            Multimap<Publisher,ContentRef> equivalents = deserializeEquivalents(context, obj.get("equivalents"));
            return new EquivalenceSummary(subject, parent, candidates, equivalents);
        }

        private String deserializeParent(JsonElement parent) {
            return parent == null ? null : parent.getAsString();
        }

        private Multimap<Publisher, ContentRef> deserializeEquivalents(
                JsonDeserializationContext context, JsonElement equivs) {
            JsonObject jsonObject = (JsonObject) equivs;
            ImmutableMultimap.Builder<Publisher, ContentRef> builder = ImmutableMultimap.builder();
            Set<Map.Entry<String, JsonElement>> entries = jsonObject.entrySet();
            for (Map.Entry<String, JsonElement> entry : entries) {
                PersistentEquivalencesContentRefs contentRefWithEquivs = deserializeContentRef(context, entry.getValue());
                Publisher publisher = Publisher.fromKey(entry.getKey()).requireValue();
                if(contentRefWithEquivs.getEquivalents() != null) {
                    for (ContentRef ref : contentRefWithEquivs.getEquivalents()) {
                        builder.put(publisher, ref);
                    }
                } else {
                    ContentRef contentRef = new ContentRef(contentRefWithEquivs.getCanonicalUri(), contentRefWithEquivs.getPublisher(), contentRefWithEquivs.getParentUri());
                    builder.put(publisher,contentRef);
                }
            }
            return builder.build();
        }

        private PersistentEquivalencesContentRefs deserializeContentRef(JsonDeserializationContext context,
                JsonElement contentRef) {
            return context.deserialize(contentRef, equivalentContentRefsTypeToken.getType());
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
        ImmutableMultimap<Publisher, ContentRef> equivalents = summary.getEquivalents();
        ContentRef firstContentRef = equivalents.values().stream().findFirst().get();
        ImmutableMap.Builder<Publisher, PersistentEquivalencesContentRefs> builder = ImmutableMap.builder();
        for (Map.Entry<Publisher, Collection<ContentRef>> entry : equivalents.asMap()
                .entrySet()) {
            PersistentEquivalencesContentRefs contentRefs = getEquivalentContentRefs(firstContentRef, entry);
            builder.put(entry.getKey(), contentRefs);
        }
        PersistenceEquivalenceSummary summaryWithMultimap =
                new PersistenceEquivalenceSummary.Builder()
                        .withSubject(summary.getSubject())
                        .withParent(summary.getParent())
                        .withCandidates(summary.getCandidates())
                        .withEquivalents(builder.build()).build();

        return gson.toJson(summaryWithMultimap).getBytes();
    }

    private PersistentEquivalencesContentRefs getEquivalentContentRefs(
            ContentRef firstContentRef, Map.Entry<Publisher, Collection<ContentRef>> entry) {
        return PersistentEquivalencesContentRefs.builder()
                .withSubject(firstContentRef.getCanonicalUri())
                .withParentUri(firstContentRef.getParentUri())
                .withPublisher(firstContentRef.getPublisher())
                .withEquivalents(ImmutableList.copyOf(entry.getValue()))
                .build();
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

    private static class PersistenceEquivalenceSummary {
        private final String subject;
        private final String parent;
        private final ImmutableList<String> candidates;
        private final ImmutableMap<Publisher, PersistentEquivalencesContentRefs> equivalents;

        private PersistenceEquivalenceSummary(String subject, String parent,
                ImmutableList<String> candidates,
                ImmutableMap<Publisher, PersistentEquivalencesContentRefs> equivalents) {
            this.subject = subject;
            this.parent = parent;
            this.candidates = candidates;
            this.equivalents = equivalents;
        }

        public String getSubject() {
            return subject;
        }

        public String getParent() {
            return parent;
        }

        public ImmutableList<String> getCandidates() {
            return candidates;
        }

        public ImmutableMap<Publisher, PersistentEquivalencesContentRefs> getEquivalents() {
            return equivalents;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {

            private String subject;
            private String parent;
            private ImmutableList<String> candidates;
            private ImmutableMap<Publisher, PersistentEquivalencesContentRefs> equivalents;

            private Builder() {
            }

            public Builder withSubject(String subject) {
                this.subject = subject;
                return this;
            }

            public Builder withParent(String parent) {
                this.parent = parent;
                return this;
            }

            public Builder withCandidates(ImmutableList<String> candidates) {
                this.candidates = candidates;
                return this;
            }

            public Builder withEquivalents(
                    ImmutableMap<Publisher, PersistentEquivalencesContentRefs> equivalents) {
                this.equivalents = equivalents;
                return this;
            }

            public PersistenceEquivalenceSummary build() {
                return new PersistenceEquivalenceSummary(subject, parent, candidates, equivalents);
            }
        }

    }

    private static class PersistentEquivalencesContentRefs {
        private final String canonicalUri;
        private final Publisher publisher;
        private final String parentUri;
        private final List<ContentRef> equivalents;

        private PersistentEquivalencesContentRefs(String canonicalUri, Publisher publisher, String parentUri,
                List<ContentRef> equivalents) {
            this.canonicalUri = canonicalUri;
            this.publisher = publisher;
            this.parentUri = parentUri;
            this.equivalents = equivalents;
        }

        public Publisher getPublisher() {
            return publisher;
        }

        public String getParentUri() {
            return parentUri;
        }

        public List<ContentRef> getEquivalents() {
            return equivalents;
        }

        public String getCanonicalUri() {
            return canonicalUri;
        }

        public static Builder builder() {
            return new PersistentEquivalencesContentRefs.Builder();
        }

        public static final class Builder {

            private String subject;
            private Publisher publisher;
            private String parentUri;
            private List<ContentRef> equivalents;

            private Builder() {
            }

            public Builder withSubject(String subject) {
                this.subject = subject;
                return this;
            }

            public Builder withParentUri(String parentUri) {
                this.parentUri = parentUri;
                return this;
            }

            public Builder withPublisher(Publisher publisher) {
                this.publisher = publisher;
                return this;
            }

            public Builder withEquivalents(List<ContentRef> equivalents) {
                this.equivalents = equivalents;
                return this;
            }

            public PersistentEquivalencesContentRefs build() {
                return new PersistentEquivalencesContentRefs(subject, publisher, parentUri, equivalents);
            }
        }
    }


}
