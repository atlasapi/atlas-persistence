package org.atlasapi.persistence.content.cassandra;

import com.google.common.collect.Sets;
import java.util.Arrays;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.ParentRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Version;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

/**
 */
@Ignore(value="Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraContentStoreTest {

    //
    //private static final String CASSANDRA_HOST = "cassandra1.owl.atlas.mbst.tv";
    private static final String CASSANDRA_HOST = "127.0.0.1";
    //
    private CassandraContentStore store;
    //

    @Before
    public void before() {
        store = new CassandraContentStore("prod", Arrays.asList(CASSANDRA_HOST), 9160, 10, 10000, 10000);
        store.init();
    }
    
    @After
    public void after() {
        store.close();
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
        
        Container container1 = new Container("container1", "curie1", Publisher.METABROADCAST);
        container1.setTitle("container1");
        container1.setId(4L);
        
        container1.setChildRefs(Arrays.asList(child1.childRef()));
        child1.setParentRef(ParentRef.parentRefFrom(container1));
        
        store.createOrUpdate(child1);
        store.createOrUpdate(container1);
        
        assertEquals(1, store.findByCanonicalUris(Arrays.asList("container1")).getAllResolvedResults().size());
        assertEquals(1, store.findByCanonicalUris(Arrays.asList("child1")).getAllResolvedResults().size());
        
        container1.setTitle("container11");
        store.createOrUpdate(container1);
    }
}
