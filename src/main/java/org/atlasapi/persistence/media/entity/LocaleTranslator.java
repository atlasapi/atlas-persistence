package org.atlasapi.persistence.media.entity;

import java.util.Locale;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;


public class LocaleTranslator {
    
    private static final String LANGUAGE_KEY = "language";
    private static final String COUNTRY_KEY = "country";

    public DBObject toDBObject(DBObject dbObject, Locale model) {
        TranslatorUtils.from(dbObject, LANGUAGE_KEY, model.getLanguage());
        TranslatorUtils.from(dbObject, COUNTRY_KEY, model.getCountry());
        
        return dbObject;
    }

    public Locale fromDBObject(DBObject dbObject) {
        String language = TranslatorUtils.toString(dbObject, LANGUAGE_KEY);
        String country = TranslatorUtils.toString(dbObject, COUNTRY_KEY);
        
        if (country == null) {
            return new Locale(language);
        }
        
        return new Locale(language, country);
    }
    
}