package org.atlasapi.persistence.content.elasticsearch;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableFutureTask;
import com.google.common.util.concurrent.SettableFuture;
import java.io.IOException;
import java.io.StringReader;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;
import org.apache.lucene.util.Version;
import org.atlasapi.persistence.content.ContentSearcher;
import org.atlasapi.persistence.content.elasticsearch.schema.ESContent;
import org.atlasapi.persistence.content.elasticsearch.schema.ESSchema;
import org.atlasapi.persistence.content.elasticsearch.support.AvailabilityQueryBuilder;
import org.atlasapi.persistence.content.elasticsearch.support.BroadcastQueryBuilder;
import org.atlasapi.persistence.content.elasticsearch.support.FilterBuilder;
import org.atlasapi.persistence.content.elasticsearch.support.TitleQueryBuilder;
import org.atlasapi.search.model.SearchQuery;
import org.atlasapi.search.model.SearchResults;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermsFilterBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.SearchHit;

/**
 */
public class ESContentSearcher implements ContentSearcher {

    private final Node index;
    private final long timeout;

    public ESContentSearcher(Node index, long timeout) {
        this.index = index;
        this.timeout = timeout;
    }

    @Override
    public ListenableFuture<SearchResults> search(SearchQuery search) {
        // IMPLEMENT PARENT QUERY!
        
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        QueryBuilder filteringQuery = null;
        QueryBuilder finalQuery = null;
        
        QueryBuilder containerQuery = null; // must consider parent
        QueryBuilder itemQuery = null; // must consider parent as null

        if (search.getTerm() != null && !search.getTerm().isEmpty()) {
            boolQuery.must(TitleQueryBuilder.build(search.getTerm(), search.getTitleWeighting()));
        }
        if (search.getCatchupWeighting() != 0.0f) {
            boolQuery.should(AvailabilityQueryBuilder.build(new Date(), search.getCatchupWeighting()));
        }

        List<TermsFilterBuilder> filters = new LinkedList<TermsFilterBuilder>();
        if (search.getIncludedPublishers() != null && !search.getIncludedPublishers().isEmpty()) {
            filters.add(FilterBuilder.buildForPublishers(search.getIncludedPublishers()));
        }
        if (search.getIncludedSpecializations() != null && !search.getIncludedSpecializations().isEmpty()) {
            filters.add(FilterBuilder.buildForSpecializations(search.getIncludedSpecializations()));
        }
        filteringQuery = QueryBuilders.filteredQuery(boolQuery, FilterBuilders.andFilter(filters.toArray(new org.elasticsearch.index.query.FilterBuilder[filters.size()])));

        if (search.getBroadcastWeighting() != 0.0f) {
            finalQuery = BroadcastQueryBuilder.build(filteringQuery, search.getBroadcastWeighting(), 1f);
        } else {
            finalQuery = filteringQuery;
        }

        final SettableFuture<SearchResults> result = SettableFuture.create();
        index.client().prepareSearch(ESSchema.INDEX_NAME).setQuery(finalQuery).execute(new ActionListener<SearchResponse>() {

            @Override
            public void onResponse(SearchResponse response) {
                Iterable<String> uris = Iterables.transform(response.getHits(), new Function<SearchHit, String>() {

                    @Override
                    public String apply(SearchHit input) {
                        return input.field(ESContent.URI).value();
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
