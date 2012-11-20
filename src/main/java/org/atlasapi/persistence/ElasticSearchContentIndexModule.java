package org.atlasapi.persistence;

import org.atlasapi.persistence.content.elasticsearch.ESContentIndexer;
import org.atlasapi.persistence.content.elasticsearch.ESContentSearcher;
import org.atlasapi.persistence.content.elasticsearch.EsScheduleIndex;
import org.atlasapi.persistence.content.elasticsearch.schema.ESSchema;
import org.atlasapi.persistence.topic.elasticsearch.ESTopicSearcher;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service.State;
import com.metabroadcast.common.time.SystemClock;

public class ElasticSearchContentIndexModule {

    private final ESContentIndexer contentIndexer;
    private final EsScheduleIndex scheduleIndex;
    private final ESTopicSearcher topicSearcher;
    private final ESContentSearcher contentSearcher;

    public ElasticSearchContentIndexModule(String seeds, long requestTimeout) {
        Node index = NodeBuilder.nodeBuilder().client(true).
                clusterName(ESSchema.CLUSTER_NAME).
                settings(ImmutableSettings.settingsBuilder().put("discovery.zen.ping.unicast.hosts", seeds)).
                build().start();
        this.contentIndexer = new ESContentIndexer(index, new SystemClock(), requestTimeout);
        this.scheduleIndex = new EsScheduleIndex(index, new SystemClock());
        this.topicSearcher = new ESTopicSearcher(index, requestTimeout);
        this.contentSearcher = new ESContentSearcher(index);
    }

    public void init() {
        Futures.addCallback(contentIndexer.start(), new FutureCallback<State>() {

            private final Logger log = LoggerFactory.getLogger(ElasticSearchContentIndexModule.class);

            @Override
            public void onSuccess(State result) {
                log.info("Started index module");
            }

            @Override
            public void onFailure(Throwable t) {
                log.info("Failed to start index module:", t);
            }
        });
    }

    public ESContentIndexer contentIndexer() {
        return contentIndexer;
    }

    public EsScheduleIndex scheduleIndex() {
        return scheduleIndex;
    }

    public ESTopicSearcher topicSearcher() {
        return topicSearcher;
    }
    
    public ESContentSearcher contentSearcher() {
        return contentSearcher;
    }
}
