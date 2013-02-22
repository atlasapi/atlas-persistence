package org.atlasapi.persistence.media.channel.cassandra;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import org.joda.time.Duration;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.caching.BackgroundComputingValue;
import com.metabroadcast.common.ids.IdGenerator;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.common.Id;
import org.atlasapi.persistence.cassandra.CassandraPersistenceException;
import org.atlasapi.serialization.json.JsonFactory;
import org.atlasapi.persistence.media.channel.*;
import static org.atlasapi.persistence.cassandra.CassandraSchema.*;

public class CassandraChannelStore implements ChannelResolver, ChannelWriter {

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    //
    private final AstyanaxContext<Keyspace> context;
    private final int requestTimeout;
    private final Keyspace keyspace;

    private IdGenerator idGenerator;
    private BackgroundComputingValue<Map<Id, Channel>> channels;

    public CassandraChannelStore(AstyanaxContext<Keyspace> context, int requestTimeout, IdGenerator idGenerator, Duration cacheExpiry) {
        this.context = context;
        this.requestTimeout = requestTimeout;
        this.keyspace = context.getEntity();
        this.idGenerator = idGenerator;
        this.channels = new BackgroundComputingValue<Map<Id, Channel>>(cacheExpiry, new Callable<Map<Id, Channel>>() {

            @Override
            public Map<Id, Channel> call() throws Exception {
                return getChannels();
            }
        });
        channels.start(getChannels());
    }

    @Override
    public void write(Channel channel) {
        if (channel.getId() == null) {
            channel.setId(Id.valueOf(idGenerator.generateRaw()));
        }
        try {
            MutationBatch mutation = keyspace.prepareMutationBatch();
            mutation.setConsistencyLevel(ConsistencyLevel.CL_QUORUM).
                    withRow(CHANNEL_CF, channel.getId().toString()).
                    putColumn(CHANNEL_COLUMN, mapper.writeValueAsBytes(channel));
            Future<OperationResult<Void>> result = mutation.executeAsync();
            result.get(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Maybe<Channel> fromId(Id id) {
        return Maybe.fromPossibleNullValue(channels.get().get(id));
    }

    @Override
    public Iterable<Channel> all() {
        return channels.get().values();
    }

    @Override
    public Iterable<Channel> forIds(Iterable<Id> ids) {
        com.google.common.collect.ImmutableList.Builder<Channel> returnedChannels = ImmutableList.builder();
        for (Id id : ids) {
            returnedChannels.add(channels.get().get(id));
        }
        return returnedChannels.build();
    }

    @Override
    public Maybe<Channel> fromUri(final String uri) {
        Maybe<Channel> channel = Maybe.fromPossibleNullValue(Iterables.getFirst(Iterables.filter(channels.get().values(), new Predicate<Channel>() {

            @Override
            public boolean apply(Channel input) {
                return input.getCanonicalUri().equals(uri);
            }
        }), null));

        return channel;
    }

    @Override
    public Maybe<Channel> fromKey(final String key) {
        return Maybe.fromPossibleNullValue(Iterables.getFirst(Iterables.filter(channels.get().values(), new Predicate<Channel>() {

            @Override
            public boolean apply(Channel input) {
                return input.key().equals(key);
            }
        }), null));
    }

    @Override
    public Map<String, Channel> forAliases(String aliasPrefix) {
        final Pattern prefixPattern = Pattern.compile(String.format("^%s", Pattern.quote(aliasPrefix)));
        Builder<String, Channel> channelMap = ImmutableMap.builder();
        for (Channel channel : channels.get().values()) {
            for (String alias : Iterables.filter(channel.getAliases(), Predicates.contains(prefixPattern))) {
                channelMap.put(alias, channel);
            }
        }
        return channelMap.build();
    }

    private Map<Id, Channel> getChannels() {
        try {
            AllRowsQuery<String, String> allRowsQuery = keyspace.prepareQuery(CHANNEL_CF).setConsistencyLevel(ConsistencyLevel.CL_ONE).getAllRows();
            allRowsQuery.setRowLimit(100);
            //
            final OperationResult<Rows<String, String>> result = allRowsQuery.execute();
            return Maps.uniqueIndex(Iterators.filter(Iterators.transform(result.getResult().iterator(), new Function<Row, Channel>() {

                @Override
                public Channel apply(Row input) {
                    try {
                        if (!input.getColumns().isEmpty()) {
                            return mapper.readValue(input.getColumns().getColumnByName(CHANNEL_COLUMN).getByteArrayValue(), Channel.class);
                        } else {
                            return null;
                        }
                    } catch (Exception ex) {
                        return null;
                    }
                }
            }), Predicates.notNull()), new Function<Channel, Id>() {

                @Override
                public Id apply(Channel input) {
                    return input.getId();
                }
            });
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }
}
