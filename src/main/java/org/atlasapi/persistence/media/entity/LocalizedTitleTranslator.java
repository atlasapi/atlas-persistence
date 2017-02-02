package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.LocalizedTitle;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;


public class LocalizedTitleTranslator implements ModelTranslator<LocalizedTitle> {

    protected static final String LANGUAGE_KEY = "language";
    protected static final String TITLE_KEY = "title";
    protected static final String TYPE_KEY = "type";
    
    @Override
    public DBObject toDBObject(DBObject dbObject, LocalizedTitle model) {
        TranslatorUtils.fromLocaleToLanguageTag(dbObject, LANGUAGE_KEY, model.getLocale());
        TranslatorUtils.from(dbObject, TITLE_KEY, model.getTitle());
        TranslatorUtils.from(dbObject, TYPE_KEY, model.getType());
        
        return dbObject;
    }

    @Override
    public LocalizedTitle fromDBObject(DBObject dbObject, LocalizedTitle model) {
        model.setLocale(TranslatorUtils.toLocaleFromLanguageTag(dbObject, LANGUAGE_KEY));
        model.setTitle(TranslatorUtils.toString(dbObject, TITLE_KEY));
        model.setType(TranslatorUtils.toString(dbObject, TYPE_KEY));
        
        return model;
    }
    
}
