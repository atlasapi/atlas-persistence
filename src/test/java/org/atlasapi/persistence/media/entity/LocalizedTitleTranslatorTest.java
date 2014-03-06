package org.atlasapi.persistence.media.entity;

import static org.atlasapi.persistence.media.entity.LocalizedTitleTranslator.LANGUAGE_KEY;
import static org.atlasapi.persistence.media.entity.LocalizedTitleTranslator.TITLE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Locale;

import org.atlasapi.media.entity.LocalizedTitle;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class LocalizedTitleTranslatorTest {

    @Test
    public void testTranslation() {
        LocalizedTitleTranslator translator = new LocalizedTitleTranslator();
        
        LocalizedTitle title = new LocalizedTitle();
        
        title.setLocale(new Locale("en", "US"));
        title.setTitle("Title");
        
        DBObject dbo = new BasicDBObject();
        translator.toDBObject(dbo, title);
        
        assertTrue(dbo.containsField(LANGUAGE_KEY));
        assertTrue(dbo.containsField(TITLE_KEY));
        
        assertEquals(title.getLanguageTag(), dbo.get(LANGUAGE_KEY));
        assertEquals(title.getTitle(), dbo.get(TITLE_KEY));
    }
    
}
