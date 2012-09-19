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
import org.elasticsearch.index.query.BoolQueryBuilder;
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
        BoolQueryBuilder combinedQuery = QueryBuilders.boolQuery();
                
        QueryBuilder titleQuery = null;
        if (search.getTerm() != null && !search.getTerm().isEmpty()) {
            titleQuery = TitleQueryBuilder.build(search.getTerm(), search.getTitleWeighting());
            combinedQuery.must(titleQuery);
        } else {
            throw new IllegalStateException("Search title should be provided!");
        }

        QueryBuilder availabilityQuery = null;
        if (search.getCatchupWeighting() != 0.0f) {
            availabilityQuery = AvailabilityQueryBuilder.build(new Date(), search.getCatchupWeighting());
            combinedQuery.should(availabilityQuery);
        }

        QueryBuilder finalQuery = combinedQuery;
        if (search.getBroadcastWeighting() != 0.0f) {
            finalQuery = BroadcastQueryBuilder.build(finalQuery, search.getBroadcastWeighting(), 1f);
        }

        List<TermsFilterBuilder> filters = new LinkedList<TermsFilterBuilder>();
        if (search.getIncludedPublishers() != null && !search.getIncludedPublishers().isEmpty()) {
            filters.add(FiltersBuilder.buildForPublishers(search.getIncludedPublishers()));
        }
        if (search.getIncludedSpecializations() != null && !search.getIncludedSpecializations().isEmpty()) {
            filters.add(FiltersBuilder.buildForSpecializations(search.getIncludedSpecializations()));
        }
        if (!filters.isEmpty()) {
            finalQuery = QueryBuilders.filteredQuery(finalQuery, FilterBuilders.andFilter(filters.toArray(new FilterBuilder[filters.size()])));
        }

        QueryBuilder containerQuery = QueryBuilders.filteredQuery(QueryBuilders.boolQuery().
                should(QueryBuilders.customBoostFactorQuery(QueryBuilders.boolQuery().must(titleQuery).must(QueryBuilders.termQuery(ESContent.HAS_CHILDREN, Boolean.FALSE))).boostFactor(0.001f)).
                should(QueryBuilders.topChildrenQuery(ESContent.CHILD_ITEM_TYPE, finalQuery).score("sum")),
                FilterBuilders.typeFilter(ESContent.CONTAINER_TYPE));

        QueryBuilder topItemQuery = QueryBuilders.filteredQuery(finalQuery, FilterBuilders.typeFilter(ESContent.TOP_ITEM_TYPE));

        QueryBuilder allQuery = QueryBuilders.boolQuery().should(containerQuery).should(topItemQuery);

        final SettableFuture<SearchResults> result = SettableFuture.create();
        index.prepareSearch(ESSchema.INDEX_NAME).
                setQuery(allQuery).
                addSort(SortBuilders.scoreSort().order(SortOrder.DESC)).
                setFrom(search.getSelection().getOffset()).
                setSize(search.getSelection().limitOrDefaultValue(10)).
                execute(new ActionListener<SearchResponse>() {

            @Override
            public void onResponse(SearchResponse response) {
                Iterable<String> uris = Iterables.transform(response.getHits(), new Function<SearchHit, String>() {

                    @Override
                    public String apply(SearchHit input) {
                        return input.sourceAsMap().get(ESContent.URI).toString();
                    }
                });
                result.set(new SearchResults(uris));
            }

            @Override
            public void onFailure(Throwable e) {
                result.setException(e);
            }
        });
        return result;
    }
}
