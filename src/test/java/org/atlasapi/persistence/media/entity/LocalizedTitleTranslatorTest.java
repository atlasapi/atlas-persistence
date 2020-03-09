package org.atlasapi.persistence.media.entity;

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.LocalizedTitle;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Test;

import static org.atlasapi.persistence.media.entity.DescribedTranslator.LOCALIZED_TITLES_KEY;
import static org.atlasapi.persistence.media.entity.DescribedTranslatorTest.localizedTitles;
import static org.atlasapi.persistence.media.entity.LocalizedTitleTranslator.LOCALE_KEY;
import static org.atlasapi.persistence.media.entity.LocalizedTitleTranslator.TITLE_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class LocalizedTitleTranslatorTest {

    @Test
    public void testLocalizedTitleTranslation() {
        LocalizedTitleTranslator translator = new LocalizedTitleTranslator();
        
        LocalizedTitle title = new LocalizedTitle();
        title.setTitle("Title");
        title.setLocale(Locale.forLanguageTag("en-US"));

        DBObject dbo = new BasicDBObject();
        translator.toDBObject(dbo, title);
        
        assertTrue(dbo.containsField(LOCALE_KEY));
        assertTrue(dbo.containsField(TITLE_KEY));
        
        assertEquals(title.getLanguageTag(), dbo.get(LOCALE_KEY));
        assertEquals(title.getTitle(), dbo.get(TITLE_KEY));
    }

    @Test
    public void testSerializeAndDeserializeLocalizedTitles() {
        DescribedTranslator translator = new DescribedTranslator(new IdentifiedTranslator(), new ImageTranslator());

        Film film = new Film();

        Set<LocalizedTitle> localizedTitles = localizedTitles();
        film.setLocalizedTitles(localizedTitles);

        Film deserializedFilm = new Film();
        DBObject dbObj = translator.toDBObject(null, film);
        assertTrue(dbObj.containsField(LOCALIZED_TITLES_KEY));

        translator.fromDBObject(dbObj, deserializedFilm);

        assertEquals(film.getLocalizedTitles(), deserializedFilm.getLocalizedTitles());

    }

}
