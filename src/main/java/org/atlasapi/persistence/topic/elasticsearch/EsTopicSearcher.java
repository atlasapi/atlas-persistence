package org.atlasapi.persistence.topic.elasticsearch;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.query.Selection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.topic.Topic;
import org.atlasapi.persistence.content.elasticsearch.schema.EsBroadcast;
import org.atlasapi.persistence.content.elasticsearch.schema.EsContent;
import org.atlasapi.persistence.content.elasticsearch.schema.EsSchema;
import org.atlasapi.persistence.content.elasticsearch.schema.EsTopicMapping;
import org.atlasapi.persistence.topic.TopicQueryResolver;
import org.atlasapi.persistence.topic.TopicSearcher;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.search.facet.FacetBuilders;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.facet.terms.TermsFacet;
import org.joda.time.Interval;

public class EsTopicSearcher implements TopicSearcher {

    private final Node index;
    private final long timeout;

    public EsTopicSearcher(Node index, long timeout) {
        this.index = index;
        this.timeout = timeout;
    }
    
    @Override
    public List<Topic> popularTopics(Interval interval, TopicQueryResolver resolver, Selection selection) {
        Facets facets = buildQuery(interval, selection).actionGet(timeout, TimeUnit.MILLISECONDS).getFacets();
        TermsFacet terms = facets.facet(TermsFacet.class, EsContent.TOPICS);
        List<Topic> result = new LinkedList<Topic>();
        for (TermsFacet.Entry term : Iterables.limit(Iterables.skip(terms.entries(), selection.getOffset()), selection.getLimit())) {
            Maybe<Topic> topic = resolver.topicForId(Id.valueOf(term.getTerm()));
            if (topic.hasValue()) {
                result.add(topic.requireValue());
            }
        }
        return result;
    }

    private ListenableActionFuture<SearchResponse> buildQuery(Interval interval, Selection selection) {
        ListenableActionFuture<SearchResponse> result = index.client().prepareSearch(EsSchema.INDEX_NAME).setQuery(
                QueryBuilders.nestedQuery(EsContent.BROADCASTS, QueryBuilders.rangeQuery(EsBroadcast.TRANSMISSION_TIME).from(interval.getStart()).to(interval.getEnd()))).
                addFacet(FacetBuilders.termsFacet(EsContent.TOPICS).field(EsContent.TOPICS + "." + EsTopicMapping.ID).size(selection.getOffset() + selection.getLimit())).
                execute();
        return result;
    }
    
}
