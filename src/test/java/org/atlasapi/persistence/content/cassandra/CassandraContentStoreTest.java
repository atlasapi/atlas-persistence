package org.atlasapi.persistence.content.cassandra;

import java.util.Arrays;
import java.util.Iterator;
import org.atlasapi.media.entity.Content;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentListingCriteria;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

/**
 */
@Ignore
public class CassandraContentStoreTest {
    
    private static final String CASSANDRA_HOST = "cassandra1.owl.atlas.mbst.tv";
    //
    private CassandraContentStore store;
    
    @Before
    public void before() {
        store = new CassandraContentStore(Arrays.asList(CASSANDRA_HOST), 9160, 10, 10000, 10000);
        store.init();
    }
    
    @After
    public void after() {
        store.close();
    }
    
    @Test
    public void testFindByCanonicalUris() {
    }

    @Test
    public void testListContent() {
        Iterator<Content> contents = store.listContent(ContentListingCriteria.defaultCriteria().forContent(ContentCategory.TOP_LEVEL_ITEM).build());
        while (contents.hasNext()) {
            System.out.println(contents.next());
        }
    }
}
