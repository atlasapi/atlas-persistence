package org.atlasapi.persistence.content.elasticsearch;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

import org.atlasapi.media.topic.Topic;
import org.atlasapi.media.topic.TopicIndex;
import org.atlasapi.persistence.content.elasticsearch.schema.EsAlias;
import org.atlasapi.persistence.content.elasticsearch.schema.EsObject;
import org.atlasapi.persistence.content.elasticsearch.schema.EsTopic;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.AbstractIdleService;

public class EsTopicIndex extends AbstractIdleService implements TopicIndex {

    private final Logger log = LoggerFactory.getLogger(EsTopicIndex.class);
    
    private final Node esClient;
    private final String indexName;
    private final long timeOutDuration;
    private final TimeUnit timeOutUnit;

    public EsTopicIndex(Node esClient, String indexName, long timeOutDuration, TimeUnit timeOutUnit) {
        this.esClient = checkNotNull(esClient);
        this.indexName = checkNotNull(indexName);
        this.timeOutDuration = timeOutDuration;
        this.timeOutUnit = checkNotNull(timeOutUnit);
    }

    @Override
    protected void startUp() throws Exception {
        IndicesAdminClient indices = esClient.client().admin().indices();
        IndicesExistsResponse exists = get(indices.exists(
            Requests.indicesExistsRequest(indexName)
        ));
        if (!exists.isExists()) {
            log.info("Creating index {}", indexName);
            get(indices.create(Requests.createIndexRequest(indexName)));
            get(indices.putMapping(Requests.putMappingRequest(indexName)
                .type(EsTopic.TYPE).source(Resources.toString(
                    Resources.getResource("topic-es-schema.json"), UTF_8))
            ));
        } else {
            log.info("Index {} exists", indexName);
        }
    }

    private <T> T get(ActionFuture<T> future) {
        return future.actionGet(timeOutDuration, timeOutUnit);
    }

    @Override
    protected void shutDown() throws Exception {

    }

    public void index(Topic topic) {
        IndexRequest request = Requests.indexRequest(indexName)
            .type(EsTopic.TYPE)
            .id(topic.getId().toString())
            .source(toEsTopic(topic).toMap());
        esClient.client().index(request).actionGet(timeOutDuration, timeOutUnit);
    }
    
    private EsObject toEsTopic(Topic topic) {
        return new EsTopic()
            .id(topic.getId().longValue())
            .source(topic.getPublisher())
            .aliases(Iterables.transform(topic.getAliases(), 
                new Function<String, EsAlias>(){
                    @Override
                    public EsAlias apply(String input) {
                        return new EsAlias().value(input);
                    }
                }
            ))
            .title(topic.getTitle())
            .description(topic.getDescription());
    }
}
