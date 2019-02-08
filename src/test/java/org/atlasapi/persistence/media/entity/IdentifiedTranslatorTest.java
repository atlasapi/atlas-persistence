package org.atlasapi.persistence.media.entity;

import com.mongodb.DBObject;
import junit.framework.TestCase;
import org.atlasapi.media.entity.Identified;

public class IdentifiedTranslatorTest extends TestCase {

    public void testShouldConvertCustomFields() {
        Identified desc = new Identified();
        desc.addCustomField("customField", "1");
        desc.addCustomField("customField2", "2");

        IdentifiedTranslator translator = new IdentifiedTranslator();
        DBObject dbObj = translator.toDBObject(null, desc);

        Identified description = new Identified();
        translator.fromDBObject(dbObj, description);

        assertEquals(desc.getCustomFields(), description.getCustomFields());
    }
}
