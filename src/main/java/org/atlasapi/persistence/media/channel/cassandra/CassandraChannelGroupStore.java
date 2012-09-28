package org.atlasapi.persistence.media.channel.cassandra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelGroup;
import org.atlasapi.persistence.cassandra.CassandraIndex;
import org.atlasapi.persistence.cassandra.CassandraPersistenceException;
import org.atlasapi.persistence.media.channel.ChannelGroupStore;
import org.atlasapi.serialization.json.JsonFactory;
import static org.atlasapi.persistence.cassandra.CassandraSchema.*;

public class CassandraChannelGroupStore implements ChannelGroupStore {

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    //
    private final CassandraIndex index = new CassandraIndex();
    private final AstyanaxContext<Keyspace> context;
    private final int requestTimeout;
    private final Keyspace keyspace;

    public CassandraChannelGroupStore(AstyanaxContext<Keyspace> context, int requestTimeout) {
        this.context = context;
        this.requestTimeout = requestTimeout;
        this.keyspace = context.getEntity();
    }

    @Override
    public ChannelGroup store(ChannelGroup group) {
        try {
            ChannelGroup old = findChannelGroup(group.getId());
            if (old != null) {
                removeChannelsIndex(old);
            }
            storeChannelGroup(group);
            createChannelsIndex(group);
            return group;
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Optional<ChannelGroup> channelGroupFor(Long id) {
        return Optional.fromNullable(findChannelGroup(id));
    }

    @Override
    public Iterable<ChannelGroup> channelGroupsFor(Iterable<Long> ids) {
        Set<ChannelGroup> result = new HashSet<ChannelGroup>();
        for (Long id : ids) {
            ChannelGroup found = findChannelGroup(id);
            if (found != null) {
                result.add(found);
            }
        }
        return result;
    }

    @Override
    public Iterable<ChannelGroup> channelGroups() {
        try {
            AllRowsQuery<String, String> allRowsQuery = keyspace.prepareQuery(CONTENT_GROUP_CF).
                    setConsistencyLevel(ConsistencyLevel.CL_ONE).
                    getAllRows();
            allRowsQuery.setRowLimit(100);
            //
            final OperationResult<Rows<String, String>> result = allRowsQuery.execute();
            return Iterables.filter(new Iterable<ChannelGroup>() {

                @Override
                public Iterator<ChannelGroup> iterator() {
                    return Iterators.transform(result.getResult().iterator(), new Function<Row, ChannelGroup>() {

                        @Override
                        public ChannelGroup apply(Row input) {
                            try {
                                if (!input.getColumns().isEmpty()) {
                                    return unmarshalChannelGroup(input.getColumns());
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

    @Override
    public Iterable<ChannelGroup> channelGroupsFor(Channel channel) {
        try {
            Collection<String> groups = index.inverted(keyspace, CHANNEL_GROUP_CHANNELS_INDEX_CF, ConsistencyLevel.CL_ONE).
                    lookup(channel.getId().toString()).
                    async(requestTimeout, TimeUnit.MILLISECONDS);
            Set<ChannelGroup> result = new HashSet<ChannelGroup>();
            for (String id : groups) {
                ChannelGroup group = findChannelGroup(Long.parseLong(id));
                if (group != null) {
                    result.add(group);
                }
            }
            return result;
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    private ChannelGroup findChannelGroup(Long id) {
        try {
            ColumnList<String> group = keyspace.prepareQuery(CHANNEL_GROUP_CF).
                    setConsistencyLevel(ConsistencyLevel.CL_ONE).
                    getKey(id.toString()).
                    executeAsync().
                    get(requestTimeout, TimeUnit.MILLISECONDS).
                    getResult();
            if (!group.isEmpty()) {
                return unmarshalChannelGroup(group);
            } else {
                return null;
            }
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    private ChannelGroup unmarshalChannelGroup(ColumnList columns) throws Exception {
        return mapper.readValue(columns.getColumnByName(CHANNEL_GROUP_COLUMN).getByteArrayValue(), ChannelGroup.class);
    }

    private void storeChannelGroup(ChannelGroup group) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch().setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        mutation.withRow(CHANNEL_GROUP_CF, group.getId().toString()).putColumn(CHANNEL_GROUP_COLUMN, mapper.writeValueAsBytes(group));
        mutation.executeAsync().get(requestTimeout, TimeUnit.MILLISECONDS);
    }

    private void removeChannelsIndex(ChannelGroup old) throws Exception {
        index.inverted(keyspace, CHANNEL_GROUP_CHANNELS_INDEX_CF, ConsistencyLevel.CL_QUORUM).
                from(old.getId().toString()).
                delete(Iterables.toArray(Iterables.transform(old.getChannels(), new Function<Long, String>() {

            @Override
            public String apply(Long input) {
                return input.toString();
            }
        }), String.class)).async(requestTimeout, TimeUnit.MILLISECONDS);
    }

    private void createChannelsIndex(ChannelGroup group) throws Exception {
        index.inverted(keyspace, CHANNEL_GROUP_CHANNELS_INDEX_CF, ConsistencyLevel.CL_QUORUM).
                from(group.getId().toString()).
                index(Iterables.toArray(Iterables.transform(group.getChannels(), new Function<Long, String>() {

            @Override
            public String apply(Long input) {
                return input.toString();
            }
        }), String.class)).async(requestTimeout, TimeUnit.MILLISECONDS);
    }
}
