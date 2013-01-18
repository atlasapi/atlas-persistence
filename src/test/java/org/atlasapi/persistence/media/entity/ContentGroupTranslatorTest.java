package org.atlasapi.persistence.media.entity;


import junit.framework.TestCase;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Publisher;

import com.mongodb.DBObject;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.EntityType;
import org.atlasapi.media.entity.SortKey;
import org.joda.time.DateTime;

public class ContentGroupTranslatorTest extends TestCase {

    private final ContentGroupTranslator translator = new ContentGroupTranslator();

    public void testFromGroup() throws Exception {
        ContentGroup group = new ContentGroup();
        group.setId(Id.valueOf(1));
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
        group.addContent(new ChildRef(Id.valueOf(1), SortKey.DEFAULT.toString(), new DateTime(), EntityType.ITEM));

        DBObject obj = translator.toDBObject(null, group);

        ContentGroup to = translator.fromDBObject(obj, new ContentGroup());

        assertEquals(group.getCanonicalUri(), to.getCanonicalUri());
        assertEquals(group.getDescription(), to.getDescription());
        assertEquals(group.getTitle(), to.getTitle());
        assertEquals(group.getPublisher(), to.getPublisher());
        assertEquals(group.getContents(), to.getContents());
    }
}
