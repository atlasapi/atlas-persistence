package org.atlasapi.persistence.media.segment.cassandra;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.ids.UUIDGenerator;
import java.util.Arrays;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.segment.Segment;
import org.atlasapi.media.segment.SegmentRef;
import org.atlasapi.persistence.cassandra.BaseCassandraTest;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Ignore;

/**
 *
 */
@Ignore(value = "Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraSegmentStoreTest extends BaseCassandraTest {

    private CassandraSegmentStore store;

    @Before
    @Override
    public void before() {
        super.before();
        store = new CassandraSegmentStore(context, 10000, new UUIDGenerator());
    }

    @Test
    public void testFindByIdentifier() {
        Segment segment = new Segment();
        segment.setId(Id.valueOf(1));
        segment.setIdentifier("s1");
        segment.setCanonicalUri("u1");

        store.write(segment);

        assertEquals(segment, store.resolveById(Arrays.asList(new SegmentRef("s1"))).get(new SegmentRef("s1")).requireValue());
    }

    @Test
    public void testFindByUri() {
        Segment segment = new Segment();
        segment.setId(Id.valueOf(1));
        segment.setIdentifier("s1");
        segment.setCanonicalUri("u1");
        segment.setPublisher(Publisher.METABROADCAST);

        store.write(segment);

        assertEquals(segment, store.resolveForSource(Publisher.METABROADCAST, "u1").requireValue());
    }

    @Test
    public void testFindNothing() {
        assertEquals(Maybe.nothing(), store.resolveById(Arrays.asList(new SegmentRef("notfound"))).get(new SegmentRef("notfound")));
        assertEquals(Maybe.nothing(), store.resolveForSource(Publisher.METABROADCAST, "notfound"));
    }
}
