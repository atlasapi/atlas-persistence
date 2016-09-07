package org.atlasapi.persistence.media.entity;

import static org.atlasapi.persistence.media.entity.LocalizedTitleTranslator.LANGUAGE_KEY;
import static org.atlasapi.persistence.media.entity.LocalizedTitleTranslator.TITLE_KEY;
import static org.atlasapi.persistence.media.entity.LocalizedTitleTranslator.TYPE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Locale;

import org.atlasapi.media.entity.LocalizedTitle;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class LocalizedTitleTranslatorTest {

    private LocalizedTitleTranslator translator;

    public LocalizedTitleTranslatorTest() {
        translator = new LocalizedTitleTranslator();
    }

    @Test
    public void translationOfLocalizedTitlesToDatabase() {

        LocalizedTitle title = makeLocalizedTitle();
        
        DBObject dbo = new BasicDBObject();
        translator.toDBObject(dbo, title);
        
        assertTrue(dbo.containsField(LANGUAGE_KEY));
        assertTrue(dbo.containsField(TITLE_KEY));
        assertTrue(dbo.containsField(TYPE_KEY));

        assertEquals(title.getLanguageTag(), dbo.get(LANGUAGE_KEY));
        assertEquals(title.getTitle(), dbo.get(TITLE_KEY));
        assertEquals(title.getType(), dbo.get(TYPE_KEY));
    }

    private LocalizedTitle makeLocalizedTitle() {
        LocalizedTitle title = new LocalizedTitle();
        title.setLocale(new Locale("en", "US"));
        title.setTitle("Title");
        title.setType("Some type");
        return title;
    }

    @Test
    public void translationOfLocalizedTitlesFromDatabase() {
        LocalizedTitle title = makeLocalizedTitle();

        DBObject dbo = new BasicDBObject();
        translator.toDBObject(dbo, title);

        LocalizedTitle output = translator.fromDBObject(dbo, new LocalizedTitle());

        assertEquals(title.getType(), output.getType());
        assertEquals(title.getLocale(), output.getLocale());
        assertEquals(title.getTitle(), output.getTitle());
    }
}
