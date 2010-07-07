package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Equiv;
import org.atlasapi.persistence.media.entity.EquivTranslator;

import com.mongodb.DBObject;

import junit.framework.TestCase;

public class EquivTranslatorTest extends TestCase {
    private EquivTranslator translator = new EquivTranslator();

    public void testShouldTranslateEquiv() {
        Equiv equiv = new Equiv("left", "right");
        
        DBObject dbObject = translator.toDBObject(equiv);
        Equiv resultEquiv = translator.fromDBObject(dbObject);
        
        assertEquals(equiv, resultEquiv);
        assertEquals(equiv.left(), resultEquiv.left());
        assertEquals(equiv.right(), resultEquiv.right());
    }
}
