package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.LocalizedDescription;
import org.atlasapi.persistence.ApiContentFields;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;


public class LocalizedDescriptionTranslator implements ModelTranslator<LocalizedDescription> {

    protected static final String LANGUAGE_KEY = "language";
    protected static final String DESCRIPTION_KEY = "description";
    protected static final String SHORT_DESCRIPTION_KEY = "shortDescription";
    protected static final String MEDIUM_DESCRIPTION_KEY = "mediumDescription";
    protected static final String LONG_DESCRIPTION = "longDescription";
    
    @Override
    public DBObject toDBObject(DBObject dbObject, LocalizedDescription model) {
        TranslatorUtils.fromLocaleToLanguageTag(dbObject, LANGUAGE_KEY, model.getLocale());
        TranslatorUtils.from(dbObject, SHORT_DESCRIPTION_KEY, model.getShortDescription());
        TranslatorUtils.from(dbObject, MEDIUM_DESCRIPTION_KEY, model.getMediumDescription());
        TranslatorUtils.from(dbObject, LONG_DESCRIPTION, model.getLongDescription());
        TranslatorUtils.from(dbObject, DESCRIPTION_KEY, model.getDescription());
        
        return dbObject;
    }

    @Override
    public LocalizedDescription fromDBObject(DBObject dbObject, LocalizedDescription model) {
        model.setLocale(TranslatorUtils.toLocaleFromLanguageTag(dbObject, LANGUAGE_KEY));
        model.setDescription(TranslatorUtils.toString(dbObject, DESCRIPTION_KEY));
        model.setShortDescription(TranslatorUtils.toString(dbObject, SHORT_DESCRIPTION_KEY));
        model.setMediumDescription(TranslatorUtils.toString(dbObject, MEDIUM_DESCRIPTION_KEY));
        model.setLongDescription(TranslatorUtils.toString(dbObject, LONG_DESCRIPTION));
        
        return model;
    }

    @Override
    public DBObject unsetFields(DBObject dbObject, Iterable<ApiContentFields> fieldsToUnset) {
        return dbObject;
    }

}
