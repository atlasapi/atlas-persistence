package org.atlasapi.persistence.content.cassandra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.metabroadcast.common.ids.IdGenerator;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.AllRowsQuery;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.product.Product;
import org.atlasapi.persistence.cassandra.CassandraIndex;
import org.atlasapi.persistence.cassandra.CassandraPersistenceException;
import org.atlasapi.persistence.media.product.ProductResolver;
import org.atlasapi.persistence.media.product.ProductStore;
import org.atlasapi.serialization.json.JsonFactory;
import static org.atlasapi.persistence.cassandra.CassandraSchema.*;

/**
 */
public class CassandraProductStore implements ProductStore, ProductResolver {

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    //
    private final CassandraIndex index = new CassandraIndex();
    private final AstyanaxContext<Keyspace> context;
    private final int requestTimeout;
    private final Keyspace keyspace;
    private final IdGenerator idGenerator;

    public CassandraProductStore(AstyanaxContext<Keyspace> context, int requestTimeout, IdGenerator idGenerator) {
        this.context = context;
        this.requestTimeout = requestTimeout;
        this.keyspace = context.getEntity();
        this.idGenerator = idGenerator;
    }

    @Override
    public Product store(Product product) {
        try {
            Product old = findProduct(product.getId().toString());
            if (old != null) {
                deleteUriIndex(old);
                deleteContentIndex(old);
                ensureId(product, old.getId());
            } else {
                ensureId(product, idGenerator.generateRaw());
            }
            storeProduct(product);
            createUriIndex(product);
            createContentIndex(product);
            return product;
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Optional<Product> productForSourceIdentified(Publisher source, String sourceIdentifier) {
        try {
            String id = index.direct(keyspace, PRODUCT_URI_INDEX_CF, ConsistencyLevel.CL_ONE).
                    from(sourceIdentifier).
                    lookup().async(requestTimeout, TimeUnit.MILLISECONDS);
            if (id != null) {
                Product product = findProduct(id);
                if (product != null && product.getPublisher().equals(source)) {
                    return Optional.of(product);
                } else {
                    return Optional.absent();
                }
            } else {
                return Optional.absent();
            }
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Optional<Product> productForId(long id) {
        try {
            Product product = findProduct(Long.toString(id));
            if (product != null) {
                return Optional.of(product);
            } else {
                return Optional.absent();
            }
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Iterable<Product> productsForContent(String canonicalUri) {
        try {
            Set<Product> result = new HashSet<Product>();
            Collection<String> products = index.inverted(keyspace, PRODUCT_CONTENTS_INDEX_CF, ConsistencyLevel.CL_ONE).
                    lookup(canonicalUri).async(requestTimeout, TimeUnit.MILLISECONDS);
            for (String id : products) {
                Product product = findProduct(id);
                if (product != null) {
                    result.add(product);
                }
            }
            return result;
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Iterable<Product> products() {
        try {
            AllRowsQuery<String, String> allRowsQuery = keyspace.prepareQuery(PRODUCT_CF).
                    setConsistencyLevel(ConsistencyLevel.CL_ONE).
                    getAllRows();
            allRowsQuery.setRowLimit(100);
            //
            final OperationResult<Rows<String, String>> result = allRowsQuery.execute();
            return Iterables.filter(new Iterable<Product>() {

                @Override
                public Iterator<Product> iterator() {
                    return Iterators.transform(result.getResult().iterator(), new Function<Row, Product>() {

                        @Override
                        public Product apply(Row input) {
                            try {
                                if (!input.getColumns().isEmpty()) {
                                    return unmarshalProduct(input.getColumns());
                                } else {
                                    return null;
                                }
                            } catch (Exception ex) {
                                return null;
                            }
                        }
                    });
                }
            }, Predicates.notNull());
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    private Product unmarshalProduct(ColumnList columns) throws Exception {
        return mapper.readValue(columns.getColumnByName(PRODUCT_COLUMN).getByteArrayValue(), Product.class);
    }

    private Product findProduct(String id) throws Exception {
        ColumnList<String> result = keyspace.prepareQuery(PRODUCT_CF).setConsistencyLevel(ConsistencyLevel.CL_ONE).getKey(id).executeAsync().get(requestTimeout, TimeUnit.MILLISECONDS).getResult();
        if (!result.isEmpty()) {
            return unmarshalProduct(result);
        } else {
            return null;
        }
    }

    private void deleteUriIndex(Product product) throws Exception {
        if (product.getCanonicalUri() != null) {
            index.direct(keyspace, PRODUCT_URI_INDEX_CF, ConsistencyLevel.CL_QUORUM).
                    from(product.getCanonicalUri()).
                    delete().async(requestTimeout, TimeUnit.MILLISECONDS);
        }
    }

    private void createUriIndex(Product product) throws Exception {
        if (product.getCanonicalUri() != null) {
            index.direct(keyspace, PRODUCT_URI_INDEX_CF, ConsistencyLevel.CL_QUORUM).
                    from(product.getCanonicalUri()).
                    to(product.getId().toString()).
                    index().async(requestTimeout, TimeUnit.MILLISECONDS);
        }
    }

    private void deleteContentIndex(Product old) throws Exception {
        if (!old.getContent().isEmpty()) {
            index.inverted(keyspace, PRODUCT_CONTENTS_INDEX_CF, ConsistencyLevel.CL_QUORUM).
                    from(old.getId().toString()).
                    delete(Iterables.toArray(old.getContent(), String.class)).
                    async(requestTimeout, TimeUnit.MILLISECONDS);
        }
    }

    private void createContentIndex(Product product) throws Exception {
        if (!product.getContent().isEmpty()) {
            index.inverted(keyspace, PRODUCT_CONTENTS_INDEX_CF, ConsistencyLevel.CL_QUORUM).
                    from(product.getId().toString()).
                    index(Iterables.toArray(product.getContent(), String.class)).
                    async(requestTimeout, TimeUnit.MILLISECONDS);
        }
    }

    private void storeProduct(Product product) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch().setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        mutation.withRow(PRODUCT_CF, product.getId().toString()).putColumn(PRODUCT_COLUMN, mapper.writeValueAsBytes(product));
        mutation.executeAsync().get(requestTimeout, TimeUnit.MILLISECONDS);
    }

    private void ensureId(Identified identified, Long id) {
        if (identified.getId() == null) {
            identified.setId(id);
        }
    }
}
