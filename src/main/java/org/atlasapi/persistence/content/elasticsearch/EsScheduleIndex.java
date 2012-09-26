package org.atlasapi.persistence.content.elasticsearch;

import static org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast.CHANNEL;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast.ID;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast.TRANSMISSION_END_TIME;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast.TRANSMISSION_TIME;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESContent.BROADCASTS;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESContent.PUBLISHER;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESContent.CHILD_ITEM_TYPE;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESContent.TOP_ITEM_TYPE;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESSchema.INDEX_NAME;
import static org.elasticsearch.index.query.FilterBuilders.andFilter;
import static org.elasticsearch.index.query.FilterBuilders.nestedFilter;
import static org.elasticsearch.index.query.FilterBuilders.orFilter;
import static org.elasticsearch.index.query.FilterBuilders.rangeFilter;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.nestedQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.MatchQueryBuilder.Operator.AND;

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
import org.elasticsearch.common.primitives.Ints;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHitField;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metabroadcast.common.time.DateTimeZones;

public class EsScheduleIndex implements ScheduleIndex {

    private static final String SCROLL_TIMEOUT = "1m";
    private static final int MAX_SCROLLS = 10;

    private static class HitAccumulator {
        
        private final String scrollId;
        private final long total;
        private final List<SearchHit> hits;
        private int seen = 0;
        private int queries = 1;
        private long millis = 0;
        
        private HitAccumulator(SearchResponse scanResult) {
            this.scrollId = scanResult.scrollId();
            this.total = scanResult.hits().totalHits();
            this.hits = Lists.newArrayListWithCapacity(Ints.saturatedCast(total));
            this.millis = scanResult.tookInMillis();
        }

        public void foldIn(SearchResponse scrollResponse) {
            this.seen += scrollResponse.hits().hits().length;
            this.millis += scrollResponse.tookInMillis(); 
            this.queries++;
            Iterables.addAll(this.hits, scrollResponse.hits());
        }
        
        public String scrollId() {
            return this.scrollId;
        }
        
        public boolean canScrollMore() {
            return this.seen < this.total && this.queries < MAX_SCROLLS;
        }

        public int queries() {
            return this.queries;
        }

        public Iterable<SearchHit> hits() {
            return this.hits;
        }
        
        public long millis() {
            return this.millis;
        }
    }

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
    private final AsyncFunction<SearchResponse, HitAccumulator> scrollSearchFunction = new AsyncFunction<SearchResponse, HitAccumulator>() {
        
        @Override
        public ListenableFuture<HitAccumulator> apply(SearchResponse input) throws Exception {
            log.info("{}: scan   {} hits ({}ms)", new Object[]{input.scrollId().hashCode(), input.hits().totalHits(), input.tookInMillis()});
            SettableFuture<HitAccumulator> searchResult = SettableFuture.create();
            HitAccumulator accumulator = new HitAccumulator(input);
            if (accumulator.canScrollMore()) {
                scrollSearch(input.scrollId(), scrollingListener(searchResult, accumulator));
            } else {
                searchResult.set(accumulator);
            }
            return searchResult;
        }

        private void scrollSearch(String scrollId, ActionListener<SearchResponse> listener) {
            esClient.client()
                .prepareSearchScroll(scrollId)
                .setScroll(SCROLL_TIMEOUT)
                .execute(listener);
        }

        private ActionListener<SearchResponse> scrollingListener(final SettableFuture<HitAccumulator> searchResult, final HitAccumulator accumulator) {
            return new ActionListener<SearchResponse>() {

                @Override
                public void onResponse(SearchResponse response) {
                    log.info("{}: scroll {} hits ({}ms)", new Object[]{accumulator.scrollId().hashCode(), response.hits().hits().length, response.tookInMillis()});
                                        
                    accumulator.foldIn(response);

                    if (accumulator.canScrollMore()) {
                        scrollSearch(response.scrollId(), this);
                    } else {
                        searchResult.set(accumulator);
                    }
                }

                @Override
                public void onFailure(Throwable e) {
                    searchResult.setException(e);
                }
            };
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
        
        SettableFuture<SearchResponse> result = SettableFuture.create();
        
        esClient.client()
            .prepareSearch(INDEX_NAME)
            .setTypes(CHILD_ITEM_TYPE, TOP_ITEM_TYPE)
            .setSearchType(SearchType.SCAN)
            .setScroll(SCROLL_TIMEOUT)
            .setQuery(scheduleQueryFor(pub, broadcastOn, from, to))
            .addFields(FIELDS)
            .setSize(10)
            .execute(resultSettingListener(result));
        
        return Futures.transform(Futures.transform(result, scrollSearchFunction), resultTransformer(broadcastOn, scheduleInterval));
    }
    
    private QueryBuilder scheduleQueryFor(String publisher, String broadcastOn, Date from, Date to) {
        return filteredQuery(
            boolQuery()
                .must(fieldQuery(PUBLISHER, publisher))
                .must(nestedQuery(BROADCASTS, matchQuery(CHANNEL, broadcastOn).operator(AND)))
            ,
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

    
    private Function<HitAccumulator, ScheduleRef> resultTransformer(final String channel, final Interval scheduleInterval) {
        return new Function<HitAccumulator, ScheduleRef>() {
            @Override
            public ScheduleRef apply(@Nullable HitAccumulator input) {
                ScheduleRef.Builder refBuilder = ScheduleRef.forChannel(channel);
                int hits = 0;
                for (SearchHit hit : input.hits()) {
                    hits++;
                    refBuilder.addEntries(validEntries(hit,channel, scheduleInterval));
                }
                ScheduleRef ref = refBuilder.build();
                log.info("{}: {} hits => {} entries, ({} queries, {}ms)", new Object[]{input.scrollId().hashCode(), hits, ref.getScheduleEntries().size(), input.queries(), input.millis()});
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
