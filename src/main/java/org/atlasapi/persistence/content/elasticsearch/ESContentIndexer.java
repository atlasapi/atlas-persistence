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
import org.elasticsearch.node.NodeBuilder;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESSchema.*;
import org.atlasapi.persistence.content.elasticsearch.schema.ESVersion;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.node.Node;

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
        try {
            ActionFuture<CreateIndexResponse> index = esClient.client().admin().indices().create(Requests.createIndexRequest(INDEX_NAME));
            index.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (IndexAlreadyExistsException ex) {
        }
        //
        ActionFuture<PutMappingResponse> mapped = esClient.client().admin().indices().putMapping(
                Requests.putMappingRequest(INDEX_NAME).type(ESItem.TYPE).source(
                XContentFactory.jsonBuilder().
                startObject().
                startObject(ESItem.TYPE).
                startObject("properties").
                startObject("versions").
                field("type").value("nested").
                endObject().
                endObject().
                endObject().
                endObject()));
        mapped.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void index(Item item) {
        Collection<ESVersion> esVersions = new LinkedList<ESVersion>();
        for (Version version : item.getVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                esVersions.add(new ESVersion().channel(
                        broadcast.getBroadcastOn()).
                        transmissionTime(broadcast.getTransmissionTime().toDate()).
                        transmissionEndTime(broadcast.getTransmissionEndTime().toDate()));
            }
        }
        ESItem esItem = new ESItem().uri(item.getCanonicalUri()).publisher(item.getPublisher().name()).versions(esVersions);
        ActionFuture<IndexResponse> result = esClient.client().index(
                Requests.indexRequest(INDEX_NAME).
                type(ESItem.TYPE).
                id(item.getCanonicalUri()).
                source(esItem.toMap()));
        result.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }
}
