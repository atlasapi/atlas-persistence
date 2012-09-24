package org.atlasapi.persistence.media.segment.cassandra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.segment.Segment;
import org.atlasapi.media.segment.SegmentRef;
import org.atlasapi.persistence.cassandra.CassandraIndex;
import org.atlasapi.persistence.cassandra.CassandraPersistenceException;
import org.atlasapi.persistence.media.segment.SegmentResolver;
import org.atlasapi.persistence.media.segment.SegmentWriter;
import org.atlasapi.serialization.json.JsonFactory;
import static org.atlasapi.persistence.cassandra.CassandraSchema.*;

/**
 */
public class CassandraSegmentStore implements SegmentResolver, SegmentWriter {

    private final ObjectMapper mapper = JsonFactory.makeJsonMapper();
    //
    private final CassandraIndex index = new CassandraIndex();
    private final AstyanaxContext<Keyspace> context;
    private final int requestTimeout;
    //
    private Keyspace keyspace;

    public CassandraSegmentStore(AstyanaxContext<Keyspace> context, int requestTimeout) {
        this.context = context;
        this.requestTimeout = requestTimeout;
        this.keyspace = context.getEntity();
    }

    @Override
    public Segment write(Segment segment) {
        try {
            Segment old = findSegment(segment.getIdentifier());
            if (old != null) {
                deleteUriIndex(old);
            }
            writeSegment(segment);
            createUriIndex(segment);
            return segment;
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Map<SegmentRef, Maybe<Segment>> resolveById(Iterable<SegmentRef> identifier) {
        try {
            Map<SegmentRef, Maybe<Segment>> result = new HashMap<SegmentRef, Maybe<Segment>>();
            for (SegmentRef ref : identifier) {
                Segment segment = findSegment(ref.identifier());
                result.put(ref, Maybe.fromPossibleNullValue(segment));
            }
            return result;
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    @Override
    public Maybe<Segment> resolveForSource(Publisher source, String uri) {
        try {
            String id = index.direct(keyspace, SEGMENT_SECONDARY_CF, ConsistencyLevel.CL_ONE).
                    from(uri).
                    lookup().async(requestTimeout, TimeUnit.MILLISECONDS);
            if (id != null) {
                Segment segment = findSegment(id);
                if (segment != null && segment.getPublisher().equals(source)) {
                    return Maybe.fromPossibleNullValue(segment);
                } else {
                    return Maybe.nothing();
                }
            } else {
                return Maybe.nothing();
            }
        } catch (Exception ex) {
            throw new CassandraPersistenceException(ex.getMessage(), ex);
        }
    }

    private void writeSegment(Segment segment) throws IOException, ConnectionException, InterruptedException, TimeoutException, ExecutionException {
        MutationBatch mutation = keyspace.prepareMutationBatch().setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
        mutation.withRow(SEGMENT_CF, segment.getIdentifier()).putColumn(SEGMENT_COLUMN, mapper.writeValueAsBytes(segment));
        mutation.executeAsync().get(requestTimeout, TimeUnit.MILLISECONDS);
    }

    private Segment findSegment(String id) throws Exception {
        ColumnList<String> result = keyspace.prepareQuery(SEGMENT_CF).setConsistencyLevel(ConsistencyLevel.CL_ONE).getKey(id).executeAsync().get(requestTimeout, TimeUnit.MILLISECONDS).getResult();
        if (!result.isEmpty()) {
            return mapper.readValue(result.getColumnByName(SEGMENT_COLUMN).getByteArrayValue(), Segment.class);
        } else {
            return null;
        }
    }

    private void createUriIndex(Segment segment) throws Exception {
        index.direct(keyspace, SEGMENT_SECONDARY_CF, ConsistencyLevel.CL_QUORUM).
                from(segment.getCanonicalUri()).
                to(segment.getIdentifier()).
                index().async(requestTimeout, TimeUnit.MILLISECONDS);
    }
    
    private void deleteUriIndex(Segment segment) throws Exception {
        index.direct(keyspace, SEGMENT_SECONDARY_CF, ConsistencyLevel.CL_QUORUM).
                from(segment.getCanonicalUri()).
                to(segment.getIdentifier()).
                delete().async(requestTimeout, TimeUnit.MILLISECONDS);
    }
}
