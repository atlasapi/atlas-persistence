package org.atlasapi.persistence.content.cassandra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.metabroadcast.common.base.Maybe;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.cassandra.CassandraIndex;
import org.atlasapi.persistence.cassandra.CassandraPersistenceException;
import org.atlasapi.persistence.content.ContentGroupResolver;
import org.atlasapi.persistence.content.ContentGroupWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.serialization.json.JsonFactory;
import org.atlasapi.serialization.json.configuration.model.FilteredContentGroupConfiguration;
import static org.atlasapi.persistence.cassandra.CassandraSchema.*;

/**
 */
public class CassandraContentGroupStore implements ContentGroupWriter, ContentGroupResolver {

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    //
    private final CassandraIndex index = new CassandraIndex();
    private final AstyanaxContext<Keyspace> context;
    private final int requestTimeout;
    private final Keyspace keyspace;

    public CassandraContentGroupStore(AstyanaxContext<Keyspace> context, int requestTimeout) {
        this.mapper.setFilters(new SimpleFilterProvider().addFilter(FilteredContentGroupConfiguration.FILTER, SimpleBeanPropertyFilter.serializeAllExcept(FilteredContentGroupConfiguration.CONTENTS_FILTER)));
        this.context = context;
        this.requestTimeout = requestTimeout;
        this.keyspace = context.getEntity();
    }

    @Override
    public void createOrUpdate(ContentGroup group) {
        try {
            ContentGroup old = findByUri(group.getCanonicalUri());
            if (old != null) {
                removeContentGroupIndex(old);
            }
            writeContentGroup(group);
            createContentGroupIndex(group);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public ResolvedContent findByCanonicalUris(Iterable<String> canonicalUris) {
        try {
            Map<String, Maybe<Identified>> results = new HashMap<String, Maybe<Identified>>();
            for (String uri : canonicalUris) {
                ContentGroup contentGroup = findByUri(uri);
                if (contentGroup != null) {
                    results.put(uri, Maybe.just((Identified) contentGroup));
                } else {
                    results.put(uri, Maybe.<Identified>nothing());
                }
            }
            return new ResolvedContent(results);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public ResolvedContent findByIds(Iterable<Long> ids) {
        CassandraIndex index = new CassandraIndex();
        try {
            Map<String, Maybe<Identified>> results = new HashMap<String, Maybe<Identified>>();
            for (Long id : ids) {
                String uri = index.direct(keyspace, CONTENT_GROUP_ID_INDEX_CF, ConsistencyLevel.CL_ONE).
                        from(id.toString()).
                        lookup().async(requestTimeout, TimeUnit.MILLISECONDS);
                if (uri != null) {
                    ContentGroup contentGroup = findByUri(uri);
                    if (contentGroup != null) {
                        results.put(id.toString(), Maybe.just((Identified) contentGroup));
                    } else {
                        results.put(id.toString(), Maybe.<Identified>nothing());
                    }
                } else {
                    results.put(id.toString(), Maybe.<Identified>nothing());
                }
            }
            return new ResolvedContent(results);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Iterable<ContentGroup> findAll() {
        try {
            AllRowsQuery<String, String> allRowsQuery = keyspace.prepareQuery(CONTENT_GROUP_CF).setConsistencyLevel(ConsistencyLevel.CL_ONE).getAllRows();
            allRowsQuery.setRowLimit(100);
            //
            final OperationResult<Rows<String, String>> result = allRowsQuery.execute();
            return new Iterable<ContentGroup>() {

                @Override
                public Iterator<ContentGroup> iterator() {
                    return Iterators.transform(result.getResult().iterator(), new Function<Row, ContentGroup>() {

                        @Override
                        public ContentGroup apply(Row input) {
                            try {
                                return unmarshalContentGroup(input.getColumns());
                            } catch (Exception ex) {
                                return null;
                            }
                        }
                    });
                }
            };
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    private void writeContentGroup(ContentGroup contentGroup) throws Exception {
        byte[] contentGroupBytes = mapper.writeValueAsBytes(contentGroup);
        byte[] contentsBytes = mapper.writeValueAsBytes(contentGroup.getContents());
        MutationBatch mutation = keyspace.prepareMutationBatch().setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        mutation.withRow(CONTENT_GROUP_CF, contentGroup.getCanonicalUri()).
                putColumn(CONTENT_GROUP_COLUMN, contentGroupBytes, null).
                putColumn(CONTENTS_COLUMN, contentsBytes, null);
        mutation.executeAsync().get(requestTimeout, TimeUnit.MILLISECONDS);
    }

    private void createContentGroupIndex(ContentGroup contentGroup) throws Exception {
        if (contentGroup.getId() != null) {
            index.direct(keyspace, CONTENT_GROUP_ID_INDEX_CF, ConsistencyLevel.CL_QUORUM).
                    from(contentGroup.getId().toString()).
                    to(contentGroup.getCanonicalUri()).
                    index().async(requestTimeout, TimeUnit.MILLISECONDS);
        }
    }

    private void removeContentGroupIndex(ContentGroup contentGroup) throws Exception {
        if (contentGroup.getId() != null) {
            index.direct(keyspace, CONTENT_GROUP_ID_INDEX_CF, ConsistencyLevel.CL_QUORUM).
                    from(contentGroup.getId().toString()).
                    delete().async(requestTimeout, TimeUnit.MILLISECONDS);
        }
    }

    private ContentGroup unmarshalContentGroup(ColumnList<String> columns) throws IOException {
        ContentGroup contentGroup = mapper.readValue(columns.getColumnByName(CONTENT_GROUP_COLUMN).getByteArrayValue(), ContentGroup.class);
        List<ChildRef> children = mapper.readValue(columns.getColumnByName(CONTENTS_COLUMN).getByteArrayValue(), TypeFactory.defaultInstance().constructCollectionType(List.class, ChildRef.class));
        contentGroup.setContents(children);
        return contentGroup;
    }

    private ContentGroup findByUri(String uri) throws Exception {
        Future<OperationResult<ColumnList<String>>> result = keyspace.prepareQuery(CONTENT_GROUP_CF).
                setConsistencyLevel(ConsistencyLevel.CL_ONE).
                getKey(uri).
                executeAsync();
        ColumnList<String> columns = result.get(requestTimeout, TimeUnit.MILLISECONDS).getResult();
        if (!columns.isEmpty()) {
            return unmarshalContentGroup(columns);
        } else {
            return null;
        }
    }
}
