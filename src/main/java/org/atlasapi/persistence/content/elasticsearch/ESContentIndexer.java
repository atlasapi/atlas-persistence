package org.atlasapi.persistence.content.elasticsearch;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentIndexer;
import org.atlasapi.persistence.content.elasticsearch.schema.ESItem;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESSchema.*;
import org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast;
import org.atlasapi.persistence.content.elasticsearch.schema.ESTopic;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.metabroadcast.common.time.DateTimeZones;

/**
 *
 */
public class ESContentIndexer implements ContentIndexer {

    private final Node esClient;
    private final long requestTimeout;

    public ESContentIndexer(String seeds, long requestTimeout) {
        this.esClient = NodeBuilder.nodeBuilder().client(true).clusterName(CLUSTER_NAME).
                settings(ImmutableSettings.settingsBuilder().put("discovery.zen.ping.unicast.hosts", seeds)).
                build().start();
        this.requestTimeout = requestTimeout;
    }
    
    public ESContentIndexer(Node index, long requestTimeout) {
        this.esClient = index;
        this.requestTimeout = requestTimeout;
    }

    protected ESContentIndexer(Node esClient) {
        this.esClient = esClient;
        this.requestTimeout = 60000;
    }

    public void init() throws IOException {
        ActionFuture<IndicesExistsResponse> exists = esClient.client().admin().indices().exists(Requests.indicesExistsRequest(INDEX_NAME));
        if (!exists.actionGet(requestTimeout, TimeUnit.MILLISECONDS).isExists()) {
            ActionFuture<CreateIndexResponse> create = esClient.client().admin().indices().create(Requests.createIndexRequest(INDEX_NAME));
            create.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
        }
        //
        ActionFuture<PutMappingResponse> putMapping = esClient.client().admin().indices().putMapping(
                Requests.putMappingRequest(INDEX_NAME).type(ESItem.TYPE).source(
                XContentFactory.jsonBuilder().
                startObject().
                startObject(ESItem.TYPE).
                startObject("properties").
                startObject(ESItem.BROADCASTS).
                field("type").value("nested").
                endObject().
                endObject().
                endObject().
                endObject()));
        putMapping.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void index(Item item) {
        ESItem esItem = new ESItem().uri(item.getCanonicalUri()).
                publisher(item.getPublisher().name()).
                broadcasts(makeESBroadcasts(item)).
                topics(makeESTopics(item));
        ActionFuture<IndexResponse> result = esClient.client().index(
                Requests.indexRequest(INDEX_NAME).
                type(ESItem.TYPE).
                id(item.getCanonicalUri()).
                source(esItem.toMap()));
        result.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }

    private Collection<ESBroadcast> makeESBroadcasts(Item item) {
        Collection<ESBroadcast> esBroadcasts = new LinkedList<ESBroadcast>();
        for (Version version : item.getVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                if (broadcast.isActivelyPublished()) {
                    esBroadcasts.add(new ESBroadcast().id(broadcast.getSourceId()).
                            channel(broadcast.getBroadcastOn()).
                            transmissionTime(broadcast.getTransmissionTime().toDateTime(DateTimeZones.UTC).toDate()).
                            transmissionEndTime(broadcast.getTransmissionEndTime().toDateTime(DateTimeZones.UTC).toDate()));
                }
            }
        }
        return esBroadcasts;
    }
    
    private Collection<ESTopic> makeESTopics(Item item) {
        Collection<ESTopic> esTopics = new LinkedList<ESTopic>();
        for (TopicRef topic : item.getTopicRefs()) {
            esTopics.add(new ESTopic().id(topic.getTopic()));
        }
        return esTopics;
    }
}
