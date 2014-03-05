package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.LocalizedTitle;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;


public class LocalizedTitleTranslator implements ModelTranslator<LocalizedTitle> {

    private static final String LANGUAGE_KEY = "language";
    private static final String TITLE_KEY = "title";
    
    @Override
    public DBObject toDBObject(DBObject dbObject, LocalizedTitle model) {
        TranslatorUtils.fromLocaleToLanguageTag(dbObject, LANGUAGE_KEY, model.getLocale());
        TranslatorUtils.from(dbObject, TITLE_KEY, model.getTitle());
        
        return dbObject;
    }

    @Override
    public LocalizedTitle fromDBObject(DBObject dbObject, LocalizedTitle model) {
        model.setLocale(TranslatorUtils.toLocaleFromLanguageTag(dbObject, LANGUAGE_KEY));
        model.setTitle(TranslatorUtils.toString(dbObject, TITLE_KEY));
        
        return model;
    }
    
}
