package org.atlasapi.persistence.content.elasticsearch;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
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
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

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
        Collection<ESBroadcast> esBroadcasts = new LinkedList<ESBroadcast>();
        for (Version version : item.getVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                if (broadcast.isActivelyPublished()) {
                    esBroadcasts.add(new ESBroadcast().id(broadcast.getSourceId()).
                            channel(broadcast.getBroadcastOn()).
                            transmissionTime(broadcast.getTransmissionTime().toDate()).
                            transmissionEndTime(broadcast.getTransmissionEndTime().toDate()));
                }
            }
        }
        ESItem esItem = new ESItem().uri(item.getCanonicalUri()).publisher(item.getPublisher().name()).broadcasts(esBroadcasts);
        ActionFuture<IndexResponse> result = esClient.client().index(
                Requests.indexRequest(INDEX_NAME).
                type(ESItem.TYPE).
                id(item.getCanonicalUri()).
                source(esItem.toMap()));
        result.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }
}
