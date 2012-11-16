package org.atlasapi.persistence.content.elasticsearch;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.atlasapi.persistence.content.ContentSearcher;
import org.atlasapi.persistence.content.elasticsearch.schema.ESContent;
import org.atlasapi.persistence.content.elasticsearch.schema.ESSchema;
import org.atlasapi.persistence.content.elasticsearch.support.AvailabilityQueryBuilder;
import org.atlasapi.persistence.content.elasticsearch.support.BroadcastQueryBuilder;
import org.atlasapi.persistence.content.elasticsearch.support.FiltersBuilder;
import org.atlasapi.persistence.content.elasticsearch.support.TitleQueryBuilder;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResults;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsFilterBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;

/**
 */
public class ESContentSearcher implements ContentSearcher {

    private final Client index;

    public ESContentSearcher(Node index) {
        this.index = index.client();
    }

    protected ESContentSearcher(Client index) {
        this.index = index;
    }

    @Override
    public final ListenableFuture<SearchResults> search(SearchQuery search) {
        QueryBuilder titleQuery = null;
        QueryBuilder availabilityQuery = null;
        QueryBuilder broadcastQuery = null;
        QueryBuilder contentQuery = null;

        if (search.getTerm() != null && !search.getTerm().isEmpty()) {
            titleQuery = TitleQueryBuilder.build(search.getTerm(), search.getTitleWeighting());
        } else {
            throw new IllegalStateException("Search title should be provided!");
        }

        List<TermsFilterBuilder> filters = new LinkedList<TermsFilterBuilder>();
        if (search.getIncludedPublishers() != null && !search.getIncludedPublishers().isEmpty()) {
            filters.add(FiltersBuilder.buildForPublishers(search.getIncludedPublishers()));
        }
        if (search.getIncludedSpecializations() != null && !search.getIncludedSpecializations().isEmpty()) {
            filters.add(FiltersBuilder.buildForSpecializations(search.getIncludedSpecializations()));
        }
        if (!filters.isEmpty()) {
            titleQuery = QueryBuilders.filteredQuery(titleQuery, FilterBuilders.andFilter(filters.toArray(new FilterBuilder[filters.size()])));
        }

        if (search.getBroadcastWeighting() != 0.0f) {
            broadcastQuery = BroadcastQueryBuilder.build(titleQuery, search.getBroadcastWeighting(), 1f);
        } else {
            broadcastQuery = titleQuery;
        }

        if (search.getCatchupWeighting() != 0.0f) {
            availabilityQuery = AvailabilityQueryBuilder.build(new Date(), search.getCatchupWeighting());
        }

        if (availabilityQuery != null) {
            contentQuery = QueryBuilders.boolQuery().must(broadcastQuery).should(availabilityQuery);
        } else {
            contentQuery = broadcastQuery;
        }

        QueryBuilder finalQuery = QueryBuilders.boolQuery().
                should(QueryBuilders.filteredQuery(contentQuery, FilterBuilders.andFilter(FilterBuilders.typeFilter(ESContent.TOP_LEVEL_TYPE), FilterBuilders.termFilter(ESContent.HAS_CHILDREN, Boolean.FALSE)))).
                should(QueryBuilders.topChildrenQuery(ESContent.CHILD_TYPE, contentQuery).score("sum"));

        final SettableFuture<SearchResults> result = SettableFuture.create();
        index.prepareSearch(ESSchema.INDEX_NAME).
                setQuery(finalQuery).
                addSort(SortBuilders.scoreSort().order(SortOrder.DESC)).
                setFrom(search.getSelection().getOffset()).
                setSize(search.getSelection().limitOrDefaultValue(10)).
                execute(new SearchResponseListener(result));
        return result;
    }

    private static class SearchResponseListener implements ActionListener<SearchResponse> {

        private final SettableFuture<SearchResults> result;

        public SearchResponseListener(SettableFuture<SearchResults> result) {
            this.result = result;
        }

        @Override
        public void onResponse(SearchResponse response) {
            Iterable<String> uris = Iterables.transform(response.getHits(), new SearchHitToURI());
            result.set(new SearchResults(uris));
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    private static class SearchHitToURI implements Function<SearchHit, String> {

        @Override
        public String apply(SearchHit input) {
            return input.sourceAsMap().get(ESContent.URI).toString();
        }
    }
}
