package org.atlasapi.persistence.content.cassandra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.concurrency.FutureList;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.query.AllRowsQuery;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.cassandra.CassandraPersistenceException;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.serialization.json.JsonFactory;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.lookup.NewLookupWriter;

import static org.atlasapi.persistence.cassandra.CassandraSchema.*;
import org.atlasapi.serialization.json.configuration.model.FilteredContainerConfiguration;
import org.atlasapi.serialization.json.configuration.model.FilteredItemConfiguration;

/**
 */
public class CassandraContentStore implements ContentWriter, ContentResolver, ContentLister {

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    //
    private final AstyanaxContext<Keyspace> context;
    private final int requestTimeout;
    private final Keyspace keyspace;
    private final NewLookupWriter lookupWriter;

    public CassandraContentStore(AstyanaxContext<Keyspace> context, int requestTimeout, NewLookupWriter lookupWriter) {
        this.mapper.setFilters(new SimpleFilterProvider().addFilter(FilteredItemConfiguration.FILTER, SimpleBeanPropertyFilter.serializeAllExcept(FilteredItemConfiguration.CLIPS_FILTER, FilteredItemConfiguration.VERSIONS_FILTER)).
                addFilter(FilteredContainerConfiguration.FILTER, SimpleBeanPropertyFilter.serializeAllExcept(FilteredContainerConfiguration.CLIPS_FILTER, FilteredContainerConfiguration.CHILD_REFS_FILTER)));
        this.context = context;
        this.requestTimeout = requestTimeout;
        this.keyspace = context.getEntity();
        this.lookupWriter = lookupWriter;
    }

    @Override
    public void createOrUpdate(final Item item) {
        try {
            Container container = null;
            ParentRef parent = item.getContainer();
            if (parent != null) {
                Content candidate = readContent(parent.getUri());
                if (candidate != null) {
                    if (candidate instanceof Container) {
                        container = (Container) candidate;
                    } else {
                        throw new IllegalStateException("The following content should be a container: " + parent.getUri());
                    }
                }
            }
            FutureList results = new FutureList();
            results.add(writeItem(container, item));
            results.addAll(attachItemToParent(container, item));
            results.delay(new Runnable() {

                @Override
                public void run() {
                    lookupWriter.ensureLookup(item);
                }
            });
            results.get(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public void createOrUpdate(final Container container) {
        try {
            FutureList results = new FutureList();
            results.add(writeContainer(container));
            for (Identified child : findByCanonicalUris(Iterables.transform(container.getChildRefs(), ChildRef.TO_URI)).getAllResolvedResults()) {
                if (child instanceof Item) {
                    Item item = (Item) child;
                    results.add(writeDenormalizedContainerData(container, item));
                }
            }
            results.delay(new Runnable() {

                @Override
                public void run() {
                    lookupWriter.ensureLookup(container);
                }
            });
            results.get(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public ResolvedContent findByCanonicalUris(Iterable<String> canonicalUris) {
        try {
            Map<String, Maybe<Identified>> results = new HashMap<String, Maybe<Identified>>();
            for (String uri : canonicalUris) {
                Content foundContent = readContent(uri);
                if (foundContent != null) {
                    results.put(uri, Maybe.<Identified>just(foundContent));
                } else {
                    results.put(uri, Maybe.<Identified>nothing());
                }
            }
            return new ResolvedContent(results);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Iterator<Content> listContent(final ContentListingCriteria criteria) {
        try {
            AllRowsQuery<String, String> allRowsQuery = keyspace.prepareQuery(CONTENT_CF).setConsistencyLevel(ConsistencyLevel.CL_ONE).getAllRows();
            Iterator<Content> result = Iterators.filter(Iterators.transform(
                    allRowsQuery.setRowLimit(100).execute().getResult().iterator(),
                    new Function<Row<String, String>, Content>() {

                        @Override
                        public Content apply(Row<String, String> input) {
                            try {
                                if (!input.getColumns().isEmpty()) {
                                    return unmarshalContent(input.getColumns());
                                } else {
                                    return null;
                                }
                            } catch (Exception ex) {
                                return null;
                            }
                        }
                    }), Predicates.notNull());
            return Iterators.filter(result, new Predicate<Content>() {

                @Override
                public boolean apply(Content input) {
                    if (criteria.getPublishers().isEmpty() || (input.getPublisher() != null && criteria.getPublishers().contains(input.getPublisher()))) {
                        if (criteria.getCategories().contains(ContentCategory.CHILD_ITEM)) {
                            if ((input instanceof Item) && ((Item) input).getContainer() != null) {
                                return true;
                            }
                        }
                        if (criteria.getCategories().contains(ContentCategory.TOP_LEVEL_ITEM)) {
                            if ((input instanceof Item) && ((Item) input).getContainer() == null) {
                                return true;
                            }
                        }
                        if (criteria.getCategories().contains(ContentCategory.CONTAINER)) {
                            if (input instanceof Container) {
                                return true;
                            }
                        }
                    }
                    return false;
                }
            });
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    private Future writeItem(Container container, Item item) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch();
        mutation.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        marshalItem(item, mutation);
        marshalContainerSummary(container, item, mutation);
        return mutation.executeAsync();
    }

    private Future writeContainer(Container container) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch();
        mutation.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        marshalContainer(container, mutation);
        return mutation.executeAsync();
    }

    private Future writeDenormalizedContainerData(Container container, Item item) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch();
        mutation.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        marshalContainerSummary(container, item, mutation);
        return mutation.executeAsync();
    }

    private Collection<Future> attachItemToParent(Container container, Item item) throws Exception {
        List<Future> result = new ArrayList<Future>(2);
        if (container != null) {
            container.setChildRefs(ChildRef.dedupeAndSort(Iterables.concat(container.getChildRefs(), ImmutableList.of(item.childRef()))));
            result.add(writeContainer(container));
            result.add(writeDenormalizedContainerData(container, item));
        }
        return result;
    }

    private Content readContent(String id) throws Exception {
        try {
            Future<OperationResult<ColumnList<String>>> result = keyspace.prepareQuery(CONTENT_CF).
                    setConsistencyLevel(ConsistencyLevel.CL_ONE).
                    getKey(id.toString()).
                    executeAsync();
            OperationResult<ColumnList<String>> columns = result.get(requestTimeout, TimeUnit.MILLISECONDS);
            if (!columns.getResult().isEmpty()) {
                return unmarshalContent(columns.getResult());
            } else {
                return null;
            }
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    private void marshalItem(Item item, MutationBatch mutation) throws IOException {
        byte[] itemBytes = mapper.writeValueAsBytes(item);
        byte[] clipsBytes = mapper.writeValueAsBytes(item.getClips());
        byte[] versionsBytes = mapper.writeValueAsBytes(item.getVersions());
        mutation.withRow(CONTENT_CF, item.getCanonicalUri()).
                putColumn(CONTENT_TYPE_COLUMN, EntityType.ITEM.name()).
                putColumn(ITEM_COLUMN, itemBytes, null).
                putColumn(CLIPS_COLUMN, clipsBytes, null).
                putColumn(VERSIONS_COLUMN, versionsBytes, null);
    }

    private void marshalContainerSummary(Container container, Item item, MutationBatch mutation) throws IOException {
        Item.ContainerSummary containerSummary = buildContainerSummary(container);
        if (containerSummary != null) {
            byte[] containerSummaryBytes = mapper.writeValueAsBytes(containerSummary);
            mutation.withRow(CONTENT_CF, item.getCanonicalUri()).
                    putColumn(CONTAINER_SUMMARY_COLUMN, containerSummaryBytes, null);
        }
    }

    private void marshalContainer(Container container, MutationBatch mutation) throws IOException {
        byte[] containerBytes = mapper.writeValueAsBytes(container);
        byte[] clipsBytes = mapper.writeValueAsBytes(container.getClips());
        byte[] childrenBytes = mapper.writeValueAsBytes(container.getChildRefs());
        mutation.withRow(CONTENT_CF, container.getCanonicalUri()).
                putColumn(CONTENT_TYPE_COLUMN, EntityType.CONTAINER.name()).
                putColumn(CONTAINER_COLUMN, containerBytes, null).
                putColumn(CLIPS_COLUMN, clipsBytes, null).
                putColumn(CHILDREN_COLUMN, childrenBytes, null);
    }

    private Content unmarshalContent(ColumnList<String> columns) throws IllegalStateException, IOException {
        String type = columns.getStringValue(CONTENT_TYPE_COLUMN, "");
        if (type.equals(EntityType.ITEM.name())) {
            return unmarshalItem(columns);
        } else if (type.equals(EntityType.CONTAINER.name())) {
            return unmarshalContainer(columns);
        } else {
            throw new IllegalStateException("Unknown content type: " + type);
        }
    }

    private Content unmarshalItem(ColumnList<String> columns) throws IOException {
        Item item = mapper.readValue(columns.getColumnByName(ITEM_COLUMN).getByteArrayValue(), Item.class);
        List<Clip> clips = mapper.readValue(columns.getColumnByName(CLIPS_COLUMN).getByteArrayValue(), TypeFactory.defaultInstance().constructCollectionType(List.class, Clip.class));
        Set<Version> versions = mapper.readValue(columns.getColumnByName(VERSIONS_COLUMN).getByteArrayValue(), TypeFactory.defaultInstance().constructCollectionType(Set.class, Version.class));
        Item.ContainerSummary containerSummary = columns.getColumnNames().contains(CONTAINER_SUMMARY_COLUMN)
                ? mapper.readValue(columns.getColumnByName(CONTAINER_SUMMARY_COLUMN).getByteArrayValue(), Item.ContainerSummary.class)
                : null;
        item.setClips(clips);
        item.setVersions(versions);
        item.setContainerSummary(containerSummary);
        return item;
    }

    private Container unmarshalContainer(ColumnList<String> columns) throws IOException {
        Container container = mapper.readValue(columns.getColumnByName(CONTAINER_COLUMN).getByteArrayValue(), Container.class);
        List<Clip> clips = mapper.readValue(columns.getColumnByName(CLIPS_COLUMN).getByteArrayValue(), TypeFactory.defaultInstance().constructCollectionType(List.class, Clip.class));
        List<ChildRef> children = mapper.readValue(columns.getColumnByName(CHILDREN_COLUMN).getByteArrayValue(), TypeFactory.defaultInstance().constructCollectionType(List.class, ChildRef.class));
        container.setClips(clips);
        container.setChildRefs(children);
        return container;
    }

    private Item.ContainerSummary buildContainerSummary(Container container) throws IOException {
        if (container != null) {
            String title = container.getTitle();
            String description = container.getDescription();
            Integer series = null;
            if (container instanceof Series) {
                series = ((Series) container).getSeriesNumber();
            }
            return new Item.ContainerSummary(EntityType.from(container).name(), title, description, series);
        } else {
            return null;
        }
    }
}
