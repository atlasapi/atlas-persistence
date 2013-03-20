package org.atlasapi.persistence.content.elasticsearch;

import com.metabroadcast.common.time.DateTimeZones;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentIndexer;
import org.atlasapi.persistence.content.elasticsearch.schema.ESContent;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.atlasapi.persistence.content.elasticsearch.schema.ESBroadcast;
import org.atlasapi.persistence.content.elasticsearch.schema.ESLocation;
import org.atlasapi.persistence.content.elasticsearch.schema.ESTopic;
import org.atlasapi.persistence.content.elasticsearch.support.Strings;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import static org.atlasapi.persistence.content.elasticsearch.schema.ESSchema.*;

import com.metabroadcast.common.time.DateTimeZones;

/**
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
        if (createIndex()) {
            putContainerMapping();
            putChildItemMapping();
            putTopItemMapping();
        }
    }

    @Override
    public void index(Item item) {
        ESContent indexed = new ESContent().uri(item.getCanonicalUri()).
                title(item.getTitle() != null ? item.getTitle() : null).
                flattenedTitle(item.getTitle() != null ? Strings.flatten(item.getTitle()) : null).
                parentTitle(item.getTitle() != null ? item.getTitle() : null).
                parentFlattenedTitle(item.getTitle() != null ? Strings.flatten(item.getTitle()) : null).
                publisher(item.getPublisher() != null ? item.getPublisher().key() : null).
                specialization(item.getSpecialization() != null ? item.getSpecialization().name() : null).
                broadcasts(makeESBroadcasts(item)).
                locations(makeESLocations(item)).
                topics(makeESTopics(item));
        IndexRequest request = null;
        if (item.getContainer() != null) {
            fillParentData(indexed, item.getContainer());
            request = Requests.indexRequest(INDEX_NAME).
                    type(ESContent.CHILD_ITEM_TYPE).
                    id(item.getCanonicalUri()).
                    source(indexed.toMap()).
                    parent(item.getContainer().getUri());
        } else {
            request = Requests.indexRequest(INDEX_NAME).
                    type(ESContent.TOP_ITEM_TYPE).
                    id(item.getCanonicalUri()).
                    source(indexed.toMap());
        }
        ActionFuture<IndexResponse> result = esClient.client().index(request);
        result.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void index(Container container) {
        ESContent indexed = new ESContent().uri(container.getCanonicalUri()).
                title(container.getTitle() != null ? container.getTitle() : null).
                flattenedTitle(container.getTitle() != null ? Strings.flatten(container.getTitle()) : null).
                parentTitle(container.getTitle() != null ? container.getTitle() : null).
                parentFlattenedTitle(container.getTitle() != null ? Strings.flatten(container.getTitle()) : null).
                publisher(container.getPublisher() != null ? container.getPublisher().key() : null).
                specialization(container.getSpecialization() != null ? container.getSpecialization().name() : null);
        if (!container.getChildRefs().isEmpty()) {
            indexed.hasChildren(Boolean.TRUE);
            for (ChildRef child : container.getChildRefs()) {
                indexChildData(container, child);
            }
        } else {
            indexed.hasChildren(Boolean.FALSE);
        }
        ActionFuture<IndexResponse> result = esClient.client().index(
                Requests.indexRequest(INDEX_NAME).
                type(ESContent.CONTAINER_TYPE).
                id(container.getCanonicalUri()).
                source(indexed.toMap()));
        result.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }

    private boolean createIndex() throws ElasticSearchException {
        ActionFuture<IndicesExistsResponse> exists = esClient.client().admin().indices().exists(Requests.indicesExistsRequest(INDEX_NAME));
        if (!exists.actionGet(requestTimeout, TimeUnit.MILLISECONDS).isExists()) {
            ActionFuture<CreateIndexResponse> create = esClient.client().admin().indices().create(Requests.createIndexRequest(INDEX_NAME));
            create.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
            return true;
        } else {
            return false;
        }
    }

    private void putContainerMapping() throws IOException, ElasticSearchException {
        ActionFuture<PutMappingResponse> putMapping = esClient.client().admin().indices().putMapping(
                Requests.putMappingRequest(INDEX_NAME).type(ESContent.CONTAINER_TYPE).source(
                XContentFactory.jsonBuilder().
                startObject().
                startObject(ESContent.CONTAINER_TYPE).
                startObject("properties").
                startObject(ESContent.URI).
                field("type").value("string").
                field("index").value("not_analyzed").
                endObject().
                startObject(ESContent.TITLE).
                field("type").value("string").
                field("index").value("analyzed").
                endObject().
                startObject(ESContent.FLATTENED_TITLE).
                field("type").value("string").
                field("index").value("analyzed").
                endObject().
                startObject(ESContent.PUBLISHER).
                field("type").value("string").
                field("index").value("not_analyzed").
                endObject().
                startObject(ESContent.SPECIALIZATION).
                field("type").value("string").
                field("index").value("not_analyzed").
                endObject().
                endObject().
                endObject().
                endObject()));
        putMapping.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }

    private void putChildItemMapping() throws ElasticSearchException, IOException {
        ActionFuture<PutMappingResponse> putMapping = esClient.client().admin().indices().putMapping(
                Requests.putMappingRequest(INDEX_NAME).type(ESContent.CHILD_ITEM_TYPE).source(
                XContentFactory.jsonBuilder().
                startObject().
                startObject(ESContent.CHILD_ITEM_TYPE).
                startObject("_parent").
                field("type").value(ESContent.CONTAINER_TYPE).
                endObject().
                startObject("properties").
                startObject(ESContent.URI).
                field("type").value("string").
                field("index").value("not_analyzed").
                endObject().
                startObject(ESContent.TITLE).
                field("type").value("string").
                field("index").value("analyzed").
                endObject().
                startObject(ESContent.FLATTENED_TITLE).
                field("type").value("string").
                field("index").value("analyzed").
                endObject().
                startObject(ESContent.PUBLISHER).
                field("type").value("string").
                field("index").value("not_analyzed").
                endObject().
                startObject(ESContent.SPECIALIZATION).
                field("type").value("string").
                field("index").value("not_analyzed").
                endObject().
                startObject(ESContent.BROADCASTS).
                field("type").value("nested").
                endObject().
                startObject(ESContent.LOCATIONS).
                field("type").value("nested").
                endObject().
                endObject().
                endObject().
                endObject()));
        putMapping.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }

    private void putTopItemMapping() throws ElasticSearchException, IOException {
        ActionFuture<PutMappingResponse> putMapping = esClient.client().admin().indices().putMapping(
                Requests.putMappingRequest(INDEX_NAME).type(ESContent.TOP_ITEM_TYPE).source(
                XContentFactory.jsonBuilder().
                startObject().
                startObject(ESContent.TOP_ITEM_TYPE).
                startObject("properties").
                startObject(ESContent.URI).
                field("type").value("string").
                field("index").value("not_analyzed").
                endObject().
                startObject(ESContent.TITLE).
                field("type").value("string").
                field("index").value("analyzed").
                endObject().
                startObject(ESContent.FLATTENED_TITLE).
                field("type").value("string").
                field("index").value("analyzed").
                endObject().
                startObject(ESContent.PUBLISHER).
                field("type").value("string").
                field("index").value("not_analyzed").
                endObject().
                startObject(ESContent.SPECIALIZATION).
                field("type").value("string").
                field("index").value("not_analyzed").
                endObject().
                startObject(ESContent.BROADCASTS).
                field("type").value("nested").
                endObject().
                startObject(ESContent.LOCATIONS).
                field("type").value("nested").
                endObject().
                endObject().
                endObject().
                endObject()));
        putMapping.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
    }

    private Collection<ESBroadcast> makeESBroadcasts(Item item) {
        Collection<ESBroadcast> esBroadcasts = new LinkedList<ESBroadcast>();
        for (Version version : item.getVersions()) {
            for (Broadcast broadcast : version.getBroadcasts()) {
                if (broadcast.isActivelyPublished()) {
                    esBroadcasts.add(new ESBroadcast().id(broadcast.getSourceId()).
                            channel(broadcast.getBroadcastOn()).
                            transmissionTime(broadcast.getTransmissionTime().toDateTime(DateTimeZones.UTC).toDate()).
                            transmissionEndTime(broadcast.getTransmissionEndTime().toDateTime(DateTimeZones.UTC).toDate()).
                            transmissionTimeInMillis(broadcast.getTransmissionTime().toDateTime(DateTimeZones.UTC).getMillis()).
                            repeat(broadcast.getRepeat() != null ? broadcast.getRepeat() : false));
                }
            }
        }
        return esBroadcasts;
    }

    private Collection<ESLocation> makeESLocations(Item item) {
        Collection<ESLocation> esLocations = new LinkedList<ESLocation>();
        for (Version version : item.getVersions()) {
            for (Encoding encoding : version.getManifestedAs()) {
                for (Location location : encoding.getAvailableAt()) {
                    if (location.getPolicy() != null && location.getPolicy().getAvailabilityStart() != null && location.getPolicy().getAvailabilityEnd() != null) {
                        esLocations.add(new ESLocation().availabilityTime(location.getPolicy().getAvailabilityStart().toDateTime(DateTimeZones.UTC).toDate()).
                                availabilityEndTime(location.getPolicy().getAvailabilityEnd().toDateTime(DateTimeZones.UTC).toDate()));
                    }
                }
            }
        }
        return esLocations;
    }

    private Collection<ESTopic> makeESTopics(Item item) {
        Collection<ESTopic> esTopics = new LinkedList<ESTopic>();
        for (TopicRef topic : item.getTopicRefs()) {
            esTopics.add(new ESTopic().id(topic.getTopic()));
        }
        return esTopics;
    }

    private void fillParentData(ESContent child, ParentRef parent) {
        Map<String, Object> indexedContainer = trySearchParent(parent);
        if (indexedContainer != null) {
            child.parentTitle(indexedContainer.containsKey(ESContent.TITLE) ? indexedContainer.get(ESContent.TITLE).toString() : null);
            child.parentFlattenedTitle(indexedContainer.containsKey(ESContent.FLATTENED_TITLE) ? indexedContainer.get(ESContent.FLATTENED_TITLE).toString() : null);
        }
    }

    private void indexChildData(Container parent, ChildRef child) {
        Map<String, Object> indexedChild = trySearchChild(parent, child);
        if (indexedChild != null) {
            if (parent.getTitle() != null) {
                indexedChild.put(ESContent.PARENT_TITLE, parent.getTitle());
                indexedChild.put(ESContent.PARENT_FLATTENED_TITLE, Strings.flatten(parent.getTitle()));
            }
            ActionFuture<IndexResponse> result = esClient.client().index(
                    Requests.indexRequest(INDEX_NAME).
                    type(ESContent.CHILD_ITEM_TYPE).
                    parent(parent.getCanonicalUri()).
                    id(child.getUri()).
                    source(indexedChild));
            result.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
        }
    }

    private Map<String, Object> trySearchParent(ParentRef parent) {
        ActionFuture<GetResponse> future = esClient.client().get(Requests.getRequest(INDEX_NAME).id(parent.getUri()));
        GetResponse response = future.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
        if (response.exists()) {
            return response.sourceAsMap();
        } else {
            return null;
        }
    }

    private Map<String, Object> trySearchChild(Container parent, ChildRef child) {
        try {
            ActionFuture<GetResponse> future = esClient.client().get(Requests.getRequest(INDEX_NAME).parent(parent.getCanonicalUri()).id(child.getUri()));
            GetResponse response = future.actionGet(requestTimeout, TimeUnit.MILLISECONDS);
            if (response.exists()) {
                return response.sourceAsMap();
            } else {
                return null;
            }
        } catch (NoShardAvailableActionException ex) {
            return null;
        }
    }
}
