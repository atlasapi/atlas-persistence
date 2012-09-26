package org.atlasapi.persistence.content.cassandra;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import java.util.Arrays;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.cassandra.BaseCassandraTest;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 */
@Ignore(value = "Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraContentStoreTest extends BaseCassandraTest {

    private CassandraContentStore store;

    @Override
    @Before
    public void before() {
        super.before();
        store = new CassandraContentStore(context, 10000);
    }

    @Test
    public void testItems() {
        Item item1 = new Item("item1", "item1", Publisher.METABROADCAST);
        item1.setTitle("item1");
        item1.setId(1L);
        item1.setClips(Arrays.asList(new Clip("clip1", "clip1", Publisher.METABROADCAST)));
        item1.setVersions(Sets.newHashSet(new Version()));

        Item item2 = new Item("item2", "item2", Publisher.METABROADCAST);
        item2.setTitle("item2");
        item2.setId(2L);
        item2.setClips(Arrays.asList(new Clip("clip2", "clip2", Publisher.METABROADCAST)));
        item2.setVersions(Sets.newHashSet(new Version()));

        store.createOrUpdate(item1);
        store.createOrUpdate(item2);

        assertEquals(1, store.findByCanonicalUris(Arrays.asList("item1")).getAllResolvedResults().size());
        assertEquals(1, store.findByCanonicalUris(Arrays.asList("item2")).getAllResolvedResults().size());
    }

    @Test
    public void testContainerWithChild() {
        Item child1 = new Item("child1", "child1", Publisher.METABROADCAST);
        child1.setTitle("child1");
        child1.setId(3L);

        Series container1 = new Series("container1", "curie1", Publisher.METABROADCAST);
        container1.setTitle("container1");
        container1.setDescription("description1");
        container1.withSeriesNumber(1);
        container1.setId(4L);

        container1.setChildRefs(Arrays.asList(child1.childRef()));
        child1.setParentRef(ParentRef.parentRefFrom(container1));

        store.createOrUpdate(child1);
        store.createOrUpdate(container1);

        assertEquals(1, store.findByCanonicalUris(Arrays.asList("container1")).getAllResolvedResults().size());
        assertEquals(1, store.findByCanonicalUris(Arrays.asList("child1")).getAllResolvedResults().size());

        container1.setTitle("container11");
        store.createOrUpdate(container1);

        Item read = (Item) store.findByCanonicalUris(Arrays.asList("child1")).getAllResolvedResults().get(0);
        assertEquals(EntityType.SERIES.name(), read.getContainerSummary().getType());
        assertEquals("container11", read.getContainerSummary().getTitle());
        assertEquals("description1", read.getContainerSummary().getDescription());
        assertEquals(Integer.valueOf(1), read.getContainerSummary().getSeriesNumber());
    }
    
    @Test
    public void testListDifferentContentTypes() {
        Item child1 = new Item("child1", "child1", Publisher.METABROADCAST);
        child1.setTitle("child1");
        child1.setId(3L);

        Series container1 = new Series("container1", "curie1", Publisher.METABROADCAST);
        container1.setTitle("container1");
        container1.setDescription("description1");
        container1.withSeriesNumber(1);
        container1.setId(4L);

        container1.setChildRefs(Arrays.asList(child1.childRef()));
        child1.setParentRef(ParentRef.parentRefFrom(container1));

        store.createOrUpdate(child1);
        store.createOrUpdate(container1);

        assertEquals(1, Iterators.size(store.listContent(new ContentListingCriteria.Builder().forContent(ContentCategory.CONTAINER).build())));
        assertEquals(container1, store.listContent(new ContentListingCriteria.Builder().forContent(ContentCategory.CONTAINER).build()).next());
        assertEquals(1, Iterators.size(store.listContent(new ContentListingCriteria.Builder().forContent(ContentCategory.CHILD_ITEM).build())));
        assertEquals(child1, store.listContent(new ContentListingCriteria.Builder().forContent(ContentCategory.CHILD_ITEM).build()).next());
    }
    
    @Test
    public void testNoContentFound() {
        assertEquals(0, store.findByCanonicalUris(Arrays.asList("notfound")).getAllResolvedResults().size());
    }
}
