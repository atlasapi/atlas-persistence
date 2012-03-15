package org.atlasapi.persistence.content.mongo;

import com.google.common.collect.ImmutableList;
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
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.SortKey;
import org.atlasapi.persistence.content.ContentCategory;

import static org.junit.Assert.*;

public class MongoContentGroupPersistenceTest {

    private static DatabasedMongo MONGO = MongoTestHelper.anEmptyTestDatabase();
    //
    private final DBCollection table = new MongoContentTables(MONGO).collectionFor(ContentCategory.CONTENT_GROUP);
    //
    private final MongoContentGroupWriter writer = new MongoContentGroupWriter(MONGO, new SystemClock());
    private final MongoContentGroupResolver resolver = new MongoContentGroupResolver(MONGO);

    @After
    public void clearDb() {
        table.drop();
    }

    @Test
    public void testWriteContentGroupAndResolveByURI() {
        ContentGroup contentGroup = new ContentGroup("group", Publisher.BBC);
        ChildRef child1 = new ChildRef("child1", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
        ChildRef child2 = new ChildRef("child2", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
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
        ChildRef child1 = new ChildRef("child1", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
        ChildRef child2 = new ChildRef("child2", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
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
        ChildRef child1 = new ChildRef("child1", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
        ChildRef child2 = new ChildRef("child2", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM);
        contentGroup.addContents(ImmutableList.of(child1, child2));

        writer.createOrUpdate(contentGroup);

        ContentGroup found = (ContentGroup) resolver.findByCanonicalUris(ImmutableList.of("group")).getFirstValue().requireValue();

        writer.createOrUpdate(found);

        found = (ContentGroup) resolver.findByCanonicalUris(ImmutableList.of("group")).getFirstValue().requireValue();

        assertEquals(contentGroup.getThisOrChildLastUpdated(), found.getThisOrChildLastUpdated());
    }
}
