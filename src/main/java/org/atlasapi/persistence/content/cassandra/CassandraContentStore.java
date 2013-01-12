package org.atlasapi.persistence.content.cassandra;

import static org.atlasapi.persistence.cassandra.CassandraSchema.CHILDREN_COLUMN;
import static org.atlasapi.persistence.cassandra.CassandraSchema.CLIPS_COLUMN;
import static org.atlasapi.persistence.cassandra.CassandraSchema.CONTAINER_COLUMN;
import static org.atlasapi.persistence.cassandra.CassandraSchema.CONTAINER_SUMMARY_COLUMN;
import static org.atlasapi.persistence.cassandra.CassandraSchema.CONTENT_CF;
import static org.atlasapi.persistence.cassandra.CassandraSchema.CONTENT_TYPE_COLUMN;
import static org.atlasapi.persistence.cassandra.CassandraSchema.ITEM_COLUMN;
import static org.atlasapi.persistence.cassandra.CassandraSchema.VERSIONS_COLUMN;

import java.io.IOException;
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
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.atlasapi.persistence.lookup.NewLookupWriter;
import org.atlasapi.persistence.media.TranslatorContentHasher;
import org.atlasapi.serialization.json.JsonFactory;
import org.atlasapi.serialization.json.configuration.model.FilteredContainerConfiguration;
import org.atlasapi.serialization.json.configuration.model.FilteredItemConfiguration;

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
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;

/**
 */
public class CassandraContentStore implements ContentWriter, ContentResolver, ContentLister {

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    //
    private final AstyanaxContext<Keyspace> context;
    private final int requestTimeout;
    private final Keyspace keyspace;
    private final NewLookupWriter lookupWriter;
    private final TranslatorContentHasher contentHasher;

    public CassandraContentStore(AstyanaxContext<Keyspace> context, int requestTimeout, NewLookupWriter lookupWriter) {
        this.mapper.setFilters(new SimpleFilterProvider().addFilter(FilteredItemConfiguration.FILTER, SimpleBeanPropertyFilter.serializeAllExcept(FilteredItemConfiguration.CLIPS_FILTER, FilteredItemConfiguration.VERSIONS_FILTER)).
                addFilter(FilteredContainerConfiguration.FILTER, SimpleBeanPropertyFilter.serializeAllExcept(FilteredContainerConfiguration.CLIPS_FILTER, FilteredContainerConfiguration.CHILD_REFS_FILTER)));
        this.context = context;
        this.requestTimeout = requestTimeout;
        this.keyspace = context.getEntity();
        this.lookupWriter = lookupWriter;
        this.contentHasher = new TranslatorContentHasher();
    }

    @Override
    public void createOrUpdate(final Item item) {
        if (!item.hashChanged(contentHasher.hash(item))) {
            return;
        }
        try {
            FutureList results = new FutureList();
            // First ensure lookup, asynchronously:
            results.delay(new Runnable() {

                @Override
                public void run() {
                    lookupWriter.ensureLookup(item);
                }
            });
            //
            Container container = null;
            ParentRef parent = item.getContainer();
            if (parent != null) {
                Maybe<Identified> candidate = readContent(parent.getUri());
                if (candidate.hasValue()) {
                    if (candidate.requireValue() instanceof Container) {
                        container = (Container) candidate.requireValue();
                    } else {
                        throw new IllegalStateException("The following content should be a container: " + parent.getUri());
                    }
                }
            }
            results.add(writeItem(container, item).executeAsync());
            results.get(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public void createOrUpdate(final Container container) {
        if (!container.hashChanged(contentHasher.hash(container))) {
            return;
        }
        try {
            FutureList results = new FutureList();
            // First ensure lookup, asynchronously:
            results.delay(new Runnable() {

                @Override
                public void run() {
                    lookupWriter.ensureLookup(container);
                }
            });
            //
            results.add(writeContainer(container).executeAsync());
            for (String child : Iterables.transform(container.getChildRefs(), ChildRef.TO_URI)) {
                results.add(writeDenormalizedContainerData(child, container).executeAsync());
            }
            results.get(requestTimeout, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public ResolvedContent findByCanonicalUris(Iterable<String> canonicalUris) {
        try {
            return new ResolvedContent(readContents(canonicalUris));
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
                                    return unmarshalContent(input.getKey(), input.getColumns());
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

    private MutationBatch writeItem(Container container, Item item) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch();
        mutation.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        marshalItem(item, mutation);
        if (container != null) {
            container.setChildRefs(ChildRef.dedupeAndSort(Iterables.concat(container.getChildRefs(), ImmutableList.of(item.childRef()))));
            marshalContainerChildren(container, mutation);
            marshalContainerSummary(item.getCanonicalUri(), container, mutation);
        }
        return mutation;
    }

    private MutationBatch writeContainer(Container container) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch();
        mutation.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        marshalContainer(container, mutation);
        return mutation;
    }

    private MutationBatch writeDenormalizedContainerData(String item, Container container) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch();
        mutation.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        marshalContainerSummary(item, container, mutation);
        return mutation;
    }

    private Maybe<Identified> readContent(String id) throws Exception {
        Future<OperationResult<ColumnList<String>>> result = keyspace.prepareQuery(CONTENT_CF).
                setConsistencyLevel(ConsistencyLevel.CL_ONE).
                getKey(id.toString()).
                executeAsync();
        OperationResult<ColumnList<String>> columns = result.get(requestTimeout, TimeUnit.MILLISECONDS);
        if (!columns.getResult().isEmpty()) {
            return Maybe.<Identified>just(unmarshalContent(id, columns.getResult()));
        } else {
            return Maybe.nothing();
        }
    }

    private Map<String, Maybe<Identified>> readContents(Iterable<String> ids) throws Exception {
        Map<String, Maybe<Identified>> contents = new HashMap<String, Maybe<Identified>>();
        // Avoid doing a self-DoS by partitioning the requested ids and hence doing multiple requests:
        FutureList<OperationResult<Rows<String, String>>> futures = new FutureList<OperationResult<Rows<String, String>>>();
        for (Iterable<String> partition : Iterables.partition(ids, 50)) {
            futures.add(keyspace.prepareQuery(CONTENT_CF).
                    setConsistencyLevel(ConsistencyLevel.CL_ONE).
                    getKeySlice(partition).
                    executeAsync());
        }
        List<OperationResult<Rows<String, String>>> allRows = futures.get(requestTimeout, TimeUnit.MILLISECONDS);
        for (OperationResult<Rows<String, String>> rows : allRows) {
            for (Row<String, String> row : rows.getResult()) {
                if (!row.getColumns().isEmpty()) {
                    contents.put(row.getKey(), Maybe.<Identified>just(unmarshalContent(row.getKey(), row.getColumns())));
                }
            }
        }
        // Sacrifice CPU performance in favor of memory with this other for:
        for (String id : ids) {
            if (!contents.containsKey(id)) {
                contents.put(id, Maybe.<Identified>nothing());
            }
        }
        return contents;
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

    private void marshalContainerSummary(String item, Container container, MutationBatch mutation) throws IOException {
        Item.ContainerSummary containerSummary = buildContainerSummary(container);
        if (containerSummary != null) {
            byte[] containerSummaryBytes = mapper.writeValueAsBytes(containerSummary);
            mutation.withRow(CONTENT_CF, item).
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

    private void marshalContainerChildren(Container container, MutationBatch mutation) throws IOException {
        byte[] childrenBytes = mapper.writeValueAsBytes(container.getChildRefs());
        mutation.withRow(CONTENT_CF, container.getCanonicalUri()).
                putColumn(CHILDREN_COLUMN, childrenBytes, null);
    }

    private Content unmarshalContent(String key, ColumnList<String> columns) throws IllegalStateException, IOException {
        String type = columns.getStringValue(CONTENT_TYPE_COLUMN, null);
        if (type != null) {
            if (type.equals(EntityType.ITEM.name())) {
                return unmarshalItem(columns);
            } else if (type.equals(EntityType.CONTAINER.name())) {
                return unmarshalContainer(columns);
            } else {
                throw new IllegalStateException("Unknown content type: " + type + " with id: " + key);
            }
        } else {
            try {
                return unmarshalItem(columns);
            } catch (Exception ie) {
                try {
                    return unmarshalContainer(columns);
                } catch (Exception ce) {
                    throw new IllegalStateException("Failed to deserialize: " + key);
                }
            }
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
        item.setReadHash(contentHasher.hash(item));
        return item;
    }

    private Container unmarshalContainer(ColumnList<String> columns) throws IOException {
        Container container = mapper.readValue(columns.getColumnByName(CONTAINER_COLUMN).getByteArrayValue(), Container.class);
        List<Clip> clips = mapper.readValue(columns.getColumnByName(CLIPS_COLUMN).getByteArrayValue(), TypeFactory.defaultInstance().constructCollectionType(List.class, Clip.class));
        List<ChildRef> children = mapper.readValue(columns.getColumnByName(CHILDREN_COLUMN).getByteArrayValue(), TypeFactory.defaultInstance().constructCollectionType(List.class, ChildRef.class));
        container.setClips(clips);
        container.setChildRefs(children);
        container.setReadHash(contentHasher.hash(container));
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

    @Override
    public ResolvedContent findByIds(Iterable<Long> ids) {
        throw new UnsupportedOperationException();
    }
}
