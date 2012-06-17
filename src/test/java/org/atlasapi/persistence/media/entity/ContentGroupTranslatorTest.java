package org.atlasapi.persistence.media.entity;


import junit.framework.TestCase;

import org.atlasapi.media.content.ChildRef;
import org.atlasapi.media.content.ContentGroup;
import org.atlasapi.media.content.EntityType;
import org.atlasapi.media.content.Publisher;
import org.atlasapi.media.content.SortKey;

import com.mongodb.DBObject;
import org.joda.time.DateTime;

public class ContentGroupTranslatorTest extends TestCase {

    private final ContentGroupTranslator translator = new ContentGroupTranslator();

    public void testFromGroup() throws Exception {
        ContentGroup group = new ContentGroup();
        group.setId(1L);
        group.setCanonicalUri("uri");


        DBObject obj = translator.toDBObject(null, group);
        assertEquals(1L, obj.get(IdentifiedTranslator.ID));
        assertEquals("uri", obj.get(IdentifiedTranslator.CANONICAL_URL));
    }

    public void testToGroup() throws Exception {
        ContentGroup group = new ContentGroup();
        group.setCanonicalUri("uri");
        group.setDescription("description");
        group.setTitle("title");
        group.setPublisher(Publisher.BBC);
        group.addContent(new ChildRef("child", SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM));

        DBObject obj = translator.toDBObject(null, group);

        ContentGroup to = translator.fromDBObject(obj, new ContentGroup());

        assertEquals(group.getCanonicalUri(), to.getCanonicalUri());
        assertEquals(group.getDescription(), to.getDescription());
        assertEquals(group.getTitle(), to.getTitle());
        assertEquals(group.getPublisher(), to.getPublisher());
        assertEquals(group.getContents(), to.getContents());
    }
}
