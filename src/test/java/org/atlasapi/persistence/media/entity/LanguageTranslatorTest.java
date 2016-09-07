package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Language;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class LanguageTranslatorTest {

    protected static final String CODE_KEY = "code";
    protected static final String DISPLAY_KEY = "display";
    protected static final String DUBBING_KEY = "dubbing";
    LanguageTranslator languageTranslator;

    public LanguageTranslatorTest() {
        languageTranslator = new LanguageTranslator();
    }

    @Test
    public void translationOfLanguageToDatabaseObject() {
        Language language = makeLanguage();
        DBObject dbo = new BasicDBObject();
        languageTranslator.toDBObject(dbo, language);

        assertTrue(dbo.containsField(CODE_KEY));
        assertTrue(dbo.containsField(DISPLAY_KEY));
        assertTrue(dbo.containsField(DUBBING_KEY));

        assertEquals(language.getCode(), dbo.get(CODE_KEY));
        assertEquals(language.getDisplay(), dbo.get(DISPLAY_KEY));
        assertEquals(language.getDubbing(), dbo.get(DUBBING_KEY));
    }

    @Test
    public void translationOfLanguageFromDatabaseObject() {
        Language language = makeLanguage();
        DBObject dbo = new BasicDBObject();
        languageTranslator.toDBObject(dbo, language);

        Language output = languageTranslator.fromDBObject(dbo);

        assertEquals(language.getDubbing(), output.getDubbing());
        assertEquals(language.getDisplay(), output.getDisplay());
        assertEquals(language.getCode(), output.getCode());
    }

    public Language makeLanguage() {
        return Language.builder()
                .withDisplay("display")
                .withDubbing("dubbing")
                .withCode("code")
                .build();
    }
}