package org.atlasapi.persistence.content.elasticsearch;

import static org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast.CHANNEL;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast.ID;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast.TRANSMISSION_END_TIME;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast.TRANSMISSION_TIME;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESContent.BROADCASTS;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESContent.PUBLISHER;
import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.nestedFilter;
import static org.elasticsearch.index.query.FilterBuilders.orFilter;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.textQuery;
import static org.elasticsearch.index.query.MatchQueryBuilder.Operator.AND;

import java.util.Date;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.elasticsearch.schema.ESSchema;
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
import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metabroadcast.common.time.DateTimeZones;

public class EsScheduleIndex implements ScheduleIndex {

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
    private final AsyncFunction<SearchResponse, SearchResponse> scrollSearchFunction = new AsyncFunction<SearchResponse, SearchResponse>() {
        @Override
        public ListenableFuture<SearchResponse> apply(SearchResponse input) throws Exception {
            SettableFuture<SearchResponse> searchResult = SettableFuture.create();
            esClient.client()
                .prepareSearchScroll(input.getScrollId())
                .execute(resultSettingListener(searchResult));
            return searchResult;
        }
    };

    public EsScheduleIndex(Node esClient) {
        this.esClient = esClient;
    }
    
    @Override
    public ListenableFuture<ScheduleRef> resolveSchedule(Publisher publisher, Channel channel, Interval scheduleInterval) {
        String broadcastOn = channel.getCanonicalUri();
        Date from = scheduleInterval.getStart().toDate();
        Date to = scheduleInterval.getEnd().toDate();
        String pub = publisher.name();
        
        SettableFuture<SearchResponse> scanResult = SettableFuture.create();
        
        esClient.client()
            .prepareSearch(ESSchema.INDEX_NAME)
            .setSearchType(SearchType.SCAN)
            .setScroll("1m")
            .setQuery(scheduleQueryFor(pub, broadcastOn, from, to))
            .addFields(FIELDS)
            .execute(resultSettingListener(scanResult));
        
        ListenableFuture<SearchResponse> result = Futures.transform(scanResult, scrollSearchFunction);
        
        return Futures.transform(result, resultTransformer(broadcastOn, scheduleInterval));
    }
    
    private QueryBuilder scheduleQueryFor(String publisher, String broadcastOn, Date from, Date to) {
        return filteredQuery(
            boolQuery()
                .must(fieldQuery(PUBLISHER, publisher))
                .must(nestedQuery(BROADCASTS, textQuery(CHANNEL, broadcastOn).operator(AND))), 
            nestedFilter(BROADCASTS, orFilter(andFilter(
                //inside or overlapping the interval ends
                rangeFilter(TRANSMISSION_TIME).lt(to),
                rangeFilter(TRANSMISSION_END_TIME).gt(from)
            ), andFilter(
                //containing the interval
                rangeFilter(TRANSMISSION_TIME).lte(from),
                rangeFilter(TRANSMISSION_END_TIME).gte(to)
            )))
        );
    }

    private ActionListener<SearchResponse> resultSettingListener(final SettableFuture<SearchResponse> result) {
        return new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse response) {
                result.set(response);
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
                for (SearchHit hit : input.hits()) {
                    refBuilder.addEntries(validEntries(hit,channel, scheduleInterval));
                }
                return refBuilder.build();
            }
        };
    }
    
    private Iterable<ScheduleRefEntry> validEntries(SearchHit hit, String channel, Interval scheduleInterval) {
        ImmutableList.Builder<ScheduleRefEntry> entries = ImmutableList.builder();
        
        SearchHitField broadcastsHitField = hit.field("broadcasts");
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
