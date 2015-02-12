package org.atlasapi.persistence.content.mongo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Test;

import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.SystemClock;
import com.mongodb.DBCollection;

import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.SortKey;
import org.atlasapi.persistence.audit.NoLoggingPersistenceAuditLog;
import org.atlasapi.persistence.audit.PersistenceAuditLog;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.lookup.entry.LookupEntry;

import static org.junit.Assert.*;

public class MongoContentGroupPersistenceTest {

    private static DatabasedMongo MONGO = MongoTestHelper.anEmptyTestDatabase();
    private final DBCollection table = new MongoContentTables(MONGO).collectionFor(ContentCategory.CONTENT_GROUP);
    
    private final MongoContentGroupWriter writer = new MongoContentGroupWriter(MONGO, new NoLoggingPersistenceAuditLog(), new SystemClock());
    private final MongoContentGroupResolver resolver = new MongoContentGroupResolver(MONGO);

    @After
    public void clearDb() {
        table.drop();
    }

    @Test
    public void testWriteContentGroupAndResolveByURI() {
        ContentGroup contentGroup = new ContentGroup("group", Publisher.BBC);
        ChildRef child1 = new ChildRef(null, "child1", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
        ChildRef child2 = new ChildRef(null, "child2", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
        contentGroup.addContents(ImmutableList.of(child1, child2));

        writer.createOrUpdate(contentGroup);

        ContentGroup found = (ContentGroup) resolver.findByCanonicalUris(ImmutableList.of("group")).getFirstValue().requireValue();
        assertNotNull(found.getId());
        assertEquals("group", found.getCanonicalUri());
        assertEquals(Publisher.BBC, found.getPublisher());
        assertEquals(2, contentGroup.getContents().size());
    }

    @Test
    public void testWriteContentGroupAndResolveByID() {
        ContentGroup contentGroup = new ContentGroup("group", Publisher.BBC);
        ChildRef child1 = new ChildRef(null, "child1", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
        ChildRef child2 = new ChildRef(null, "child2", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
        contentGroup.addContents(ImmutableList.of(child1, child2));

        writer.createOrUpdate(contentGroup);

        ContentGroup found = (ContentGroup) resolver.findByIds(ImmutableList.of(contentGroup.getId())).getFirstValue().requireValue();
        assertNotNull(found.getId());
        assertEquals("group", found.getCanonicalUri());
        assertEquals(Publisher.BBC, found.getPublisher());
        assertEquals(2, contentGroup.getContents().size());
    }

    @Test
    public void testContentGroupIsWrittenOnlyOnceIfUnchanged() {
        ContentGroup contentGroup = new ContentGroup("group", Publisher.BBC);
        ChildRef child1 = new ChildRef(null, "child1", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
        ChildRef child2 = new ChildRef(null, "child2", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
        contentGroup.addContents(ImmutableList.of(child1, child2));

        writer.createOrUpdate(contentGroup);

        ContentGroup found = (ContentGroup) resolver.findByCanonicalUris(ImmutableList.of("group")).getFirstValue().requireValue();

        writer.createOrUpdate(found);

        found = (ContentGroup) resolver.findByCanonicalUris(ImmutableList.of("group")).getFirstValue().requireValue();

        assertEquals(contentGroup.getThisOrChildLastUpdated(), found.getThisOrChildLastUpdated());
    }
    
    @Test
    public void testWriteContentGroupAndFindAll() {
        ContentGroup contentGroup1 = new ContentGroup("group1", Publisher.BBC);
        ChildRef child1 = new ChildRef(null, "child1", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
        contentGroup1.addContents(ImmutableList.of(child1));
        
        ContentGroup contentGroup2 = new ContentGroup("group2", Publisher.BBC);
        ChildRef child2 = new ChildRef(null, "child2", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
        contentGroup2.addContents(ImmutableList.of(child2));

        writer.createOrUpdate(contentGroup1);
        writer.createOrUpdate(contentGroup2);

        Iterable<ContentGroup> results = resolver.findAll();
        assertEquals(2, Iterables.size(results));
    }
}
