package org.atlasapi.persistence.content.elasticsearch;

import com.google.common.base.Splitter;
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
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.indices.IndexAlreadyExistsException;

/**
 *
 */
public class ESContentIndexer implements ContentIndexer {

    private final TransportClient esClient;
    private final long requestTimeout;

    public ESContentIndexer(String seeds, long requestTimeout) {
        Iterable<String> hosts = Splitter.on(",").split(seeds);
        this.esClient = new TransportClient(ImmutableSettings.settingsBuilder().put("cluster.name", CLUSTER_NAME));
        for (String host : hosts) {
            String[] pair = host.split(":");
            this.esClient.addTransportAddress(new InetSocketTransportAddress(pair[0], Integer.parseInt(pair[1])));
        }
        this.requestTimeout = requestTimeout;
    }

    public void init() throws IOException {
        try {
            ActionFuture<CreateIndexResponse> index = esClient.admin().indices().create(Requests.createIndexRequest(INDEX_NAME));
            index.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (IndexAlreadyExistsException ex) {
        }
        //
        ActionFuture<PutMappingResponse> mapped = esClient.admin().indices().putMapping(
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
        mapped.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
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
        ActionFuture<IndexResponse> result = esClient.index(
                Requests.indexRequest(INDEX_NAME).
                type(ESItem.TYPE).
                id(item.getCanonicalUri()).
                source(esItem.toMap()));
        result.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }
}
