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

    private final Node index;

    public ESContentSearcher(Node index) {
        this.index = index;
    }

    @Override
    public final ListenableFuture<SearchResults> search(SearchQuery search) {
        BoolQueryBuilder initialQuery = QueryBuilders.boolQuery().must(QueryBuilders.matchAllQuery());
        QueryBuilder finalQuery = null;

        if (search.getTerm() != null && !search.getTerm().isEmpty()) {
            initialQuery.must(TitleQueryBuilder.build(search.getTerm(), search.getTitleWeighting()));
        }

        if (search.getCatchupWeighting() != 0.0f) {
            initialQuery.should(AvailabilityQueryBuilder.build(new Date(), search.getCatchupWeighting()));
        }

        if (search.getBroadcastWeighting() != 0.0f) {
            finalQuery = BroadcastQueryBuilder.build(initialQuery, search.getBroadcastWeighting(), 1f);
        } else {
            finalQuery = initialQuery;
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
                should(QueryBuilders.boolQuery().mustNot(QueryBuilders.hasChildQuery(ESContent.CHILD_ITEM_TYPE, QueryBuilders.matchAllQuery()))).
                should(QueryBuilders.topChildrenQuery(ESContent.CHILD_ITEM_TYPE, finalQuery)),
                FilterBuilders.typeFilter(ESContent.CONTAINER_TYPE));

        QueryBuilder topItemQuery = QueryBuilders.filteredQuery(finalQuery, FilterBuilders.typeFilter(ESContent.TOP_ITEM_TYPE));

        QueryBuilder allQuery = QueryBuilders.boolQuery().should(containerQuery).should(topItemQuery);

        final SettableFuture<SearchResults> result = SettableFuture.create();
        index.client().prepareSearch(ESSchema.INDEX_NAME).
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
