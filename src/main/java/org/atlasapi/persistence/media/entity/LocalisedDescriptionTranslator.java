package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.LocalisedDescription;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class LocalisedDescriptionTranslator implements ModelTranslator<LocalisedDescription> {

    private static final String LOCALE_KEY = "locale";
    private static final String TITLE_KEY = "title";
    private static final String DESCRIPTION_KEY = "description";
    private static final String SHORT_DESCRIPTION_KEY = "shortDescription";
    private static final String MEDIUM_DESCRIPTION_KEY = "mediumDescription";
    private static final String LONG_DESCRIPTION = "longDescription";
    
    private final LocaleTranslator localeTranslator;
    
    public LocalisedDescriptionTranslator(LocaleTranslator localeTranslator) {
        this.localeTranslator = localeTranslator;
    }

    @Override
    public DBObject toDBObject(DBObject dbObject, LocalisedDescription model) {
        dbObject.put(LOCALE_KEY, localeTranslator.toDBObject(new BasicDBObject(), model.getLocale()));
        TranslatorUtils.from(dbObject, TITLE_KEY, model.getTitle());
        TranslatorUtils.from(dbObject, DESCRIPTION_KEY, model.getTitle());
        TranslatorUtils.from(dbObject, SHORT_DESCRIPTION_KEY, model.getShortDescription());
        TranslatorUtils.from(dbObject, MEDIUM_DESCRIPTION_KEY, model.getMediumDescription());
        TranslatorUtils.from(dbObject, LONG_DESCRIPTION, model.getLongDescription());
        
        return dbObject;
    }

    @Override
    public LocalisedDescription fromDBObject(DBObject dbObject, LocalisedDescription model) {
        model.setLocale(localeTranslator.fromDBObject((DBObject) dbObject.get(LOCALE_KEY)));
        model.setTitle(TranslatorUtils.toString(dbObject, TITLE_KEY));
        model.setDescription(TranslatorUtils.toString(dbObject, DESCRIPTION_KEY));
        model.setShortDescription(TranslatorUtils.toString(dbObject, SHORT_DESCRIPTION_KEY));
        model.setMediumDescription(TranslatorUtils.toString(dbObject, MEDIUM_DESCRIPTION_KEY));
        model.setLongDescription(TranslatorUtils.toString(dbObject, LONG_DESCRIPTION));
        
        return model;
    }
    
}
