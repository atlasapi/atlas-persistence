package org.atlasapi.persistence.content.elasticsearch;

import static org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast.CHANNEL;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast.ID;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast.TRANSMISSION_END_TIME;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast.TRANSMISSION_TIME;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESContent.BROADCASTS;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESContent.CHILD_TYPE;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESContent.PUBLISHER;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESContent.TOP_LEVEL_TYPE;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESSchema.INDEX_NAME;
import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.nestedFilter;
import static org.elasticsearch.index.query.FilterBuilders.orFilter;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.FilterBuilders.termFilter;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.schedule.ScheduleIndex;
import org.atlasapi.persistence.content.schedule.ScheduleRef;
import org.atlasapi.persistence.content.schedule.ScheduleRef.ScheduleRefEntry;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;

public class EsScheduleIndex implements ScheduleIndex {

    //defines the maximum number of entries per day.
    private static final int SIZE_MULTIPLIER = 100;

    public static final Logger log = LoggerFactory.getLogger(EsScheduleIndex.class);
    
    private static final String BROADCAST_ID = BROADCASTS+"."+ID;
    private static final String BROADCAST_CHANNEL = BROADCASTS+"."+CHANNEL;
    private static final String BROADCAST_TRANSMISSION_TIME = BROADCASTS+"."+TRANSMISSION_TIME;
    private static final String BROADCAST_TRANSMISSION_END_TIME = BROADCASTS+"."+TRANSMISSION_END_TIME;
    
    private static final String[] FIELDS = new String[]{
        BROADCASTS,
        BROADCAST_ID,
        BROADCAST_CHANNEL,
        BROADCAST_TRANSMISSION_TIME,
        BROADCAST_TRANSMISSION_END_TIME,
    };
    
    private final Node esClient;
    private final EsScheduleIndexNames scheduleNames;

    public EsScheduleIndex(Node esClient, Clock clock) {
        this.esClient = esClient;
        this.scheduleNames = new EsScheduleIndexNames(clock);
    }
    
    @Override
    public ListenableFuture<ScheduleRef> resolveSchedule(Publisher publisher, Channel channel, Interval scheduleInterval) {
        String broadcastOn = channel.getCanonicalUri();
        Date from = scheduleInterval.getStart().toDate();
        Date to = scheduleInterval.getEnd().toDate();
        String pub = publisher.key();
        
        SettableFuture<SearchResponse> result = SettableFuture.create();
        
        esClient.client()
            .prepareSearch(indicesFor(scheduleInterval))
            .setTypes(TOP_LEVEL_TYPE, CHILD_TYPE)
            .setSearchType(SearchType.DEFAULT)
            .setQuery(scheduleQueryFor(pub, broadcastOn, from, to))
            .addFields(FIELDS)
            .setSize(SIZE_MULTIPLIER*daysIn(scheduleInterval))
            .execute(resultSettingListener(result));
        
        return Futures.transform(result, resultTransformer(broadcastOn, scheduleInterval));
    }
    
    private String[] indicesFor(Interval scheduleInterval) {
        ImmutableSet<String> indices = scheduleNames.queryingNamesFor(scheduleInterval.getStart(), scheduleInterval.getEnd());
        return indices.toArray(new String[indices.size()]);
    }

    private int daysIn(Interval scheduleInterval) {
        return Math.max(1, Days.daysIn(scheduleInterval).getDays());
    }

    private QueryBuilder scheduleQueryFor(String publisher, String broadcastOn, Date from, Date to) {
        return filteredQuery(
            boolQuery()
                .must(termQuery(PUBLISHER, publisher))
                .must(nestedQuery(BROADCASTS, termQuery(CHANNEL, broadcastOn)))
            ,
            nestedFilter(BROADCASTS, andFilter(
                termFilter(CHANNEL, broadcastOn),
                orFilter(andFilter(
                    //inside or overlapping the interval ends
                    rangeFilter(TRANSMISSION_TIME).lt(to),
                    rangeFilter(TRANSMISSION_END_TIME).gt(from)
                ), andFilter(
                    //containing the interval
                    rangeFilter(TRANSMISSION_TIME).lte(from),
                    rangeFilter(TRANSMISSION_END_TIME).gte(to)
                ))
            ))
        );
    }

    private <T> ActionListener<T> resultSettingListener(final SettableFuture<T> result) {
        return new ActionListener<T>() {
            @Override
            public void onResponse(T input) {
                result.set(input);
            }
            
            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        };
    }

    
    private Function<SearchResponse, ScheduleRef> resultTransformer(final String channel, final Interval scheduleInterval) {
        return new Function<SearchResponse, ScheduleRef>() {
            @Override
            public ScheduleRef apply(@Nullable SearchResponse input) {
                ScheduleRef.Builder refBuilder = ScheduleRef.forChannel(channel);
                int hits = 0;
                for (SearchHit hit : input.hits()) {
                    hits++;
                    refBuilder.addEntries(validEntries(hit,channel, scheduleInterval));
                }
                ScheduleRef ref = refBuilder.build();
                log.info("{}: {} hits => {} entries, ({} queries, {}ms)", new Object[]{Thread.currentThread().getId(), hits, ref.getScheduleEntries().size(), 1, input.tookInMillis()});
                return ref;
            }
        };
    }
    
    private Iterable<ScheduleRefEntry> validEntries(SearchHit hit, String channel, Interval scheduleInterval) {
        ImmutableList.Builder<ScheduleRefEntry> entries = ImmutableList.builder();
        
        SearchHitField broadcastsHitField = hit.field(BROADCASTS);
        String id = hit.id();
        List<Object> fieldValues = broadcastsHitField.getValues();
        for (List<?> fieldValue : Iterables.filter(fieldValues, List.class)) {
            for (Map<Object,Object> broadcast : Iterables.filter(fieldValue, Map.class)) {
                ScheduleRefEntry validRef = getValidRef(id, channel, scheduleInterval, broadcast);
                if (validRef != null) {
                    entries.add(validRef);
                }
            }
        }        
        return entries.build();
    }

    private ScheduleRefEntry getValidRef(String id, String channel, Interval scheduleInterval, Map<Object, Object> broadcast) {
        String broadcastChannel = (String) broadcast.get(CHANNEL);
        if (channel.equals(broadcastChannel)) {
            DateTime start = new DateTime(broadcast.get(TRANSMISSION_TIME)).toDateTime(DateTimeZones.UTC);
            DateTime end = new DateTime(broadcast.get(TRANSMISSION_END_TIME)).toDateTime(DateTimeZones.UTC);
            if (valid(scheduleInterval, start, end)) {
                String broadcastId = (String) broadcast.get(BROADCAST_ID);
                return new ScheduleRefEntry(id, channel, start, end, broadcastId);
            }
        }
        return null;
    }

    private boolean valid(Interval scheduleInterval, DateTime start, DateTime end) {
        return start.isBefore(scheduleInterval.getEnd()) 
            && end.isAfter(scheduleInterval.getStart())
            || !start.isAfter(scheduleInterval.getStart()) 
            && !end.isBefore(scheduleInterval.getEnd());
    }

}
