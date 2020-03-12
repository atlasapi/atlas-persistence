package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.LocalizedTitle;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;

import com.mongodb.DBObject;


public class LocalizedTitleTranslator implements ModelTranslator<LocalizedTitle> {

    protected static final String LOCALE_KEY = "locale";
    protected static final String TITLE_KEY = "title";
    
    @Override
    public DBObject toDBObject(DBObject dbObject, LocalizedTitle model) {
        TranslatorUtils.fromLocaleToLanguageTag(dbObject, LOCALE_KEY, model.getLocale());
        TranslatorUtils.from(dbObject, TITLE_KEY, model.getTitle());

        return dbObject;
    }

    @Override
    public LocalizedTitle fromDBObject(DBObject dbObject, LocalizedTitle model) {
        model.setLocale(TranslatorUtils.toLocaleFromLanguageTag(dbObject, LOCALE_KEY));
        model.setTitle(TranslatorUtils.toString(dbObject, TITLE_KEY));

        return model;
    }
    
}
