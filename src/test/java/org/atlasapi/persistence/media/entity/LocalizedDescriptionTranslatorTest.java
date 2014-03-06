package org.atlasapi.persistence.media.entity;

import static org.atlasapi.persistence.media.entity.LocalizedDescriptionTranslator.DESCRIPTION_KEY;
import static org.atlasapi.persistence.media.entity.LocalizedDescriptionTranslator.LANGUAGE_KEY;
import static org.atlasapi.persistence.media.entity.LocalizedDescriptionTranslator.LONG_DESCRIPTION;
import static org.atlasapi.persistence.media.entity.LocalizedDescriptionTranslator.MEDIUM_DESCRIPTION_KEY;
import static org.atlasapi.persistence.media.entity.LocalizedDescriptionTranslator.SHORT_DESCRIPTION_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Locale;

import org.atlasapi.media.entity.LocalizedDescription;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class LocalizedDescriptionTranslatorTest {

    @Test
    public void testTranslation() {
        LocalizedDescriptionTranslator translator = new LocalizedDescriptionTranslator();
        
        LocalizedDescription desc = new LocalizedDescription();
        
        desc.setLocale(new Locale("en", "US"));
        desc.setDescription("Desc 2 Medium");
        desc.setShortDescription("Desc 2");
        desc.setMediumDescription("Desc 2 Medium");
        desc.setLongDescription("Desc 2 Long");
        
        DBObject dbo = new BasicDBObject();
        translator.toDBObject(dbo, desc);
        
        assertTrue(dbo.containsField(LANGUAGE_KEY));
        assertTrue(dbo.containsField(DESCRIPTION_KEY));
        assertTrue(dbo.containsField(SHORT_DESCRIPTION_KEY));
        assertTrue(dbo.containsField(MEDIUM_DESCRIPTION_KEY));
        assertTrue(dbo.containsField(LONG_DESCRIPTION));
        
        assertEquals(desc.getLanguageTag(), dbo.get(LANGUAGE_KEY));
        assertEquals(desc.getDescription(), dbo.get(DESCRIPTION_KEY));
        assertEquals(desc.getShortDescription(), dbo.get(SHORT_DESCRIPTION_KEY));
        assertEquals(desc.getMediumDescription(), dbo.get(MEDIUM_DESCRIPTION_KEY));
        assertEquals(desc.getLongDescription(), dbo.get(LONG_DESCRIPTION));
    }
    
}
