package org.atlasapi.persistence.topic.cassandra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.cassandra.CassandraIndex;
import org.atlasapi.persistence.cassandra.CassandraPersistenceException;
import org.atlasapi.persistence.topic.TopicLookupResolver;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicStore;
import org.atlasapi.serialization.json.JsonFactory;
import org.atlasapi.serialization.json.configuration.model.FilteredContentGroupConfiguration;
import static org.atlasapi.persistence.cassandra.CassandraSchema.*;

/**
 */
public class CassandraTopicStore implements TopicStore, TopicLookupResolver, TopicQueryResolver {

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    //
    private final CassandraIndex index = new CassandraIndex();
    private final AstyanaxContext<Keyspace> context;
    private final int requestTimeout;
    private final Keyspace keyspace;

    public CassandraTopicStore(AstyanaxContext<Keyspace> context, int requestTimeout) {
        this.mapper.setFilters(new SimpleFilterProvider().addFilter(FilteredContentGroupConfiguration.FILTER, SimpleBeanPropertyFilter.serializeAllExcept(FilteredContentGroupConfiguration.CONTENTS_FILTER)));
        this.context = context;
        this.requestTimeout = requestTimeout;
        this.keyspace = context.getEntity();
    }

    @Override
    public void write(Topic topic) {
        try {
            Topic old = findTopic(topic.getId().toString());
            if (old != null) {
                deleteIndex(old);
            }
            writeTopic(topic);
            createIndex(topic);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Maybe<Topic> topicForId(Long id) {
        try {
            Topic topic = findTopic(id.toString());
            return Maybe.fromPossibleNullValue(topic);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Iterable<Topic> topicsForIds(Iterable<Long> ids) {
        try {
            Set<Topic> result = new HashSet<Topic>();
            for (Long id : ids) {
                Topic topic = findTopic(id.toString());
                if (topic != null) {
                    result.add(topic);
                }
            }
            return result;
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Maybe<Topic> topicFor(String namespace, String value) {
        try {
            String id = index.direct(keyspace, TOPIC_NS_VALUE_INDEX_CF, ConsistencyLevel.CL_ONE).
                    from(buildIndexKey(namespace, value)).
                    lookup().async(requestTimeout, TimeUnit.MILLISECONDS);
            if (id != null) {
                Topic topic = findTopic(id);
                return Maybe.fromPossibleNullValue(topic);
            } else {
                return Maybe.nothing();
            }
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Maybe<Topic> topicFor(Publisher publisher, String namespace, String value) {
        try {
            String id = index.direct(keyspace, TOPIC_PUBLISHER_NS_VALUE_INDEX_CF, ConsistencyLevel.CL_ONE).
                    from(buildIndexKey(publisher, namespace, value)).
                    lookup().async(requestTimeout, TimeUnit.MILLISECONDS);
            if (id != null) {
                Topic topic = findTopic(id);
                return Maybe.fromPossibleNullValue(topic);
            } else {
                return Maybe.nothing();
            }
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Iterable<Topic> topicsFor(ContentQuery query) {
        throw new UnsupportedOperationException("Not supported here.");
    }

    @Override
    public Iterable<Topic> topics() {
        try {
            AllRowsQuery<String, String> allRowsQuery = keyspace.prepareQuery(TOPIC_CF).
                    setConsistencyLevel(ConsistencyLevel.CL_ONE).
                    getAllRows();
            allRowsQuery.setRowLimit(100);
            //
            final OperationResult<Rows<String, String>> result = allRowsQuery.execute();
            return Iterables.filter(new Iterable<Topic>() {

                @Override
                public Iterator<Topic> iterator() {
                    return Iterators.transform(result.getResult().iterator(), new Function<Row, Topic>() {

                        @Override
                        public Topic apply(Row input) {
                            try {
                                if (!input.getColumns().isEmpty()) {
                                    return mapper.readValue(input.getColumns().getColumnByName(TOPIC_COLUMN).getByteArrayValue(), Topic.class);
                                } else {
                                    return null;
                                }
                            } catch (Exception ex) {
                                return null;
                            }
                        }
                    });
                }
            }, Predicates.notNull());
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    private String buildIndexKey(String namespace, String value) {
        return namespace + ":" + value;
    }

    private String buildIndexKey(Publisher publisher, String namespace, String value) {
        return publisher.name() + ":" + namespace + ":" + value;
    }

    private Topic findTopic(String id) throws Exception {
        ColumnList<String> result = keyspace.prepareQuery(TOPIC_CF).setConsistencyLevel(ConsistencyLevel.CL_ONE).getKey(id).executeAsync().get(requestTimeout, TimeUnit.MILLISECONDS).getResult();
        if (!result.isEmpty()) {
            return mapper.readValue(result.getColumnByName(TOPIC_COLUMN).getByteArrayValue(), Topic.class);
        } else {
            return null;
        }
    }

    private void writeTopic(Topic topic) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch().setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        mutation.withRow(TOPIC_CF, topic.getId().toString()).putColumn(TOPIC_COLUMN, mapper.writeValueAsBytes(topic));
        mutation.executeAsync().get(requestTimeout, TimeUnit.MILLISECONDS);
    }

    private void createIndex(Topic topic) throws Exception {
        index.direct(keyspace, TOPIC_NS_VALUE_INDEX_CF, ConsistencyLevel.CL_QUORUM).
                from(buildIndexKey(topic.getNamespace(), topic.getValue())).
                to(topic.getId().toString()).
                index().async(requestTimeout, TimeUnit.MILLISECONDS);
        if (topic.getPublisher() != null) {
            index.direct(keyspace, TOPIC_PUBLISHER_NS_VALUE_INDEX_CF, ConsistencyLevel.CL_QUORUM).
                    from(buildIndexKey(topic.getPublisher(), topic.getNamespace(), topic.getValue())).
                    to(topic.getId().toString()).
                    index().async(requestTimeout, TimeUnit.MILLISECONDS);
        }
    }

    private void deleteIndex(Topic topic) throws Exception {
        index.direct(keyspace, TOPIC_NS_VALUE_INDEX_CF, ConsistencyLevel.CL_QUORUM).
                from(buildIndexKey(topic.getNamespace(), topic.getValue())).
                delete().async(requestTimeout, TimeUnit.MILLISECONDS);
        if (topic.getPublisher() != null) {
            index.direct(keyspace, TOPIC_PUBLISHER_NS_VALUE_INDEX_CF, ConsistencyLevel.CL_QUORUM).
                    from(buildIndexKey(topic.getPublisher(), topic.getNamespace(), topic.getValue())).
                    delete().async(requestTimeout, TimeUnit.MILLISECONDS);
        }
    }
}
