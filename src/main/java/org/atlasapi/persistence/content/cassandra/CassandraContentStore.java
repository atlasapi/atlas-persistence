package org.atlasapi.persistence.content.cassandra;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.base.Maybe;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.DataRetrievalException;
import org.atlasapi.persistence.content.DataStorageException;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.cassandra.json.ContentMapper;

/**
 */
public class CassandraContentStore implements ContentWriter, ContentResolver {

    private static final String CLUSTER = "Atlas";
    private static final String KEYSPACE = "Atlas";
    private static final ColumnFamily<String, String> CONTENT_CF = new ColumnFamily<String, String>(
            "Content",
            StringSerializer.get(),
            StringSerializer.get());
    //
    private final ContentMapper contentMapper = new ContentMapper();
    //
    private final Keyspace keyspace;

    public CassandraContentStore(int port, List<String> seeds) {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder().forCluster(CLUSTER).forKeyspace(KEYSPACE).
                withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE)).
                withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(CLUSTER).setPort(port).setMaxConnsPerHost(1).
                setSeeds(Joiner.on(",").join(seeds))).
                withConnectionPoolMonitor(new CountingConnectionPoolMonitor()).
                buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();

        keyspace = context.getEntity();
    }

    @Override
    public void createOrUpdate(Item item) {
        try {
            writeItem(item);
            attachItemToParent(item);
        } catch (Exception ex) {
            throw new DataStorageException(ex);
        }
    }

    @Override
    public void createOrUpdate(Container container) {
        try {
            writeContainer(container);
        } catch (Exception ex) {
            throw new DataStorageException(ex);
        }
    }

    @Override
    public ResolvedContent findByCanonicalUris(Iterable<String> canonicalUris) {
        try {
            Map<String, Maybe<Identified>> results = new HashMap<String, Maybe<Identified>>();
            for (String uri : canonicalUris) {
                Content found = readContent(uri);
                if (found != null) {
                    results.put(uri, Maybe.<Identified>just(found));
                }
            }
            return new ResolvedContent(results);
        } catch (Exception ex) {
            throw new DataRetrievalException(ex);
        }
    }

    private void writeItem(Item item) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch();

        contentMapper.serialize(item, new CassandraColumnWriter(mutation.withRow(CONTENT_CF, item.getCanonicalUri())));

        mutation.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        mutation.execute();
        // FIXME: blocking operation, better use the async call later.
    }

    private void writeContainer(Container container) throws Exception {
        MutationBatch mutation = keyspace.prepareMutationBatch();

        contentMapper.serialize(container, new CassandraColumnWriter(mutation.withRow(CONTENT_CF, container.getCanonicalUri())));

        mutation.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        mutation.execute();
        // FIXME: blocking operation, better use the async call later.
    }

    private void attachItemToParent(Item item) throws Exception {
        ParentRef parent = item.getContainer();
        if (parent != null) {
            Container container = readContainer(parent.getUri());
            if (container != null) {
                container.setChildRefs(ChildRef.dedupeAndSort(Iterables.concat(container.getChildRefs(), ImmutableList.of(item.childRef()))));
                writeContainer(container);
            }
        }
    }

    private Content readContent(String key) throws Exception {
        OperationResult<ColumnList<String>> result = keyspace.prepareQuery(CONTENT_CF).getKey(key).execute();
        // FIXME: blocking operation, better use the async call later.

        ColumnList<String> columns = result.getResult();

        return contentMapper.deserialize(new CassandraColumnReader(columns));
    }

    private Item readItem(String key) throws Exception {
        OperationResult<ColumnList<String>> result = keyspace.prepareQuery(CONTENT_CF).getKey(key).execute();
        // FIXME: blocking operation, better use the async call later.

        ColumnList<String> columns = result.getResult();

        return contentMapper.<Item>deserialize(new CassandraColumnReader(columns));
    }

    private Container readContainer(String key) throws Exception {
        OperationResult<ColumnList<String>> result = keyspace.prepareQuery(CONTENT_CF).getKey(key).execute();
        // FIXME: blocking operation, better use the async call later.

        ColumnList<String> columns = result.getResult();

        return contentMapper.<Container>deserialize(new CassandraColumnReader(columns));
    }
}
