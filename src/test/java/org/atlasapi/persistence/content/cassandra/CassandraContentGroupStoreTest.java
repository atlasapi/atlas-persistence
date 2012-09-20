package org.atlasapi.persistence.content.cassandra;

import com.google.common.collect.Iterables;
import java.util.Arrays;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.cassandra.BaseCassandraTest;
import org.junit.Test;
import org.junit.Before;
import org.joda.time.DateTime;
import static org.junit.Assert.*;

/**
 */
//@Ignore(value = "Enable if running a local Cassandra instance with Atlas schema.")
public class CassandraContentGroupStoreTest extends BaseCassandraTest {
    
    private CassandraContentGroupStore store;
    
    @Before
    @Override
    public void before() {
        super.before();
        store = new CassandraContentGroupStore(context, 10000);
    }
    
    @Test
    public void testContentGroupByUri() {
        ContentGroup contentGroup = new ContentGroup("cg1", Publisher.METABROADCAST);
        contentGroup.addContent(new ChildRef("child1", "", new DateTime(), EntityType.ITEM));
        
        store.createOrUpdate(contentGroup);
        
        assertEquals(contentGroup, store.findByCanonicalUris(Arrays.asList("cg1")).asMap().get("cg1").requireValue());
    }
    
    @Test
    public void testContentGroupById() {
        ContentGroup contentGroup = new ContentGroup("cg1", Publisher.METABROADCAST);
        contentGroup.setId(1l);
        contentGroup.addContent(new ChildRef("child1", "", new DateTime(), EntityType.ITEM));
        
        store.createOrUpdate(contentGroup);
        
        assertEquals(contentGroup, store.findByIds(Arrays.asList(1l)).asMap().get("1").requireValue());
    }
    
    @Test
    public void testAllContentGroups() {
        ContentGroup contentGroup1 = new ContentGroup("cg1", Publisher.METABROADCAST);
        contentGroup1.setId(1l);
        contentGroup1.addContent(new ChildRef("child1", "", new DateTime(), EntityType.ITEM));
        ContentGroup contentGroup2 = new ContentGroup("cg2", Publisher.METABROADCAST);
        contentGroup1.setId(2l);
        contentGroup1.addContent(new ChildRef("child2", "", new DateTime(), EntityType.ITEM));
        
        store.createOrUpdate(contentGroup1);
        store.createOrUpdate(contentGroup2);
        
        Iterable<ContentGroup> all = store.findAll();
        assertEquals(2, Iterables.size(all));
    }
}
