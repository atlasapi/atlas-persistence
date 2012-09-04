package org.atlasapi.persistence.topic.elasticsearch;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.query.Selection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast;
import org.atlasapi.persistence.content.elasticsearch.schema.ESItem;
import org.atlasapi.persistence.content.elasticsearch.schema.ESSchema;
import org.atlasapi.persistence.content.elasticsearch.schema.ESTopic;
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

/**
 */
public class ESTopicSearcher implements TopicSearcher {

    private final Node index;
    private final long timeout;

    public ESTopicSearcher(Node index, long timeout) {
        this.index = index;
        this.timeout = timeout;
    }
    
    @Override
    public List<Topic> popularTopics(Interval interval, TopicQueryResolver resolver, Selection selection) {
        Facets facets = buildQuery(interval, selection).actionGet(timeout, TimeUnit.MILLISECONDS).getFacets();
        TermsFacet terms = facets.facet(TermsFacet.class, ESItem.TOPICS);
        List<Topic> result = new LinkedList<Topic>();
        for (TermsFacet.Entry term : Iterables.limit(Iterables.skip(terms.entries(), selection.getOffset()), selection.getLimit())) {
            Maybe<Topic> topic = resolver.topicForId(Long.parseLong(term.getTerm()));
            if (topic.hasValue()) {
                result.add(topic.requireValue());
            }
        }
        return result;
    }

    private ListenableActionFuture<SearchResponse> buildQuery(Interval interval, Selection selection) {
        System.out.println(index.client().prepareSearch(ESSchema.INDEX_NAME).setQuery(
                QueryBuilders.nestedQuery(ESItem.BROADCASTS, QueryBuilders.rangeQuery(ESBroadcast.TRANSMISSION_TIME).from(interval.getStart()).to(interval.getEnd()))).
                addFacet(FacetBuilders.termsFacet(ESItem.TOPICS).field(ESItem.TOPICS + "." + ESTopic.ID).size(selection.getOffset() + selection.getLimit())).toString());
        ListenableActionFuture<SearchResponse> result = index.client().prepareSearch(ESSchema.INDEX_NAME).setQuery(
                QueryBuilders.nestedQuery(ESItem.BROADCASTS, QueryBuilders.rangeQuery(ESBroadcast.TRANSMISSION_TIME).from(interval.getStart()).to(interval.getEnd()))).
                addFacet(FacetBuilders.termsFacet(ESItem.TOPICS).field(ESItem.TOPICS + "." + ESTopic.ID).size(selection.getOffset() + selection.getLimit())).
                execute();
        return result;
    }
    
}
