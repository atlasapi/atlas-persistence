package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Language;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;

import com.mongodb.DBObject;

public class LanguageTranslator implements ModelTranslator<Language> {

    protected static final String CODE_KEY = "code";
    protected static final String DISPLAY_KEY = "display";
    protected static final String DUBBING_KEY = "dubbing";

    @Override
    public DBObject toDBObject(DBObject dbObject, Language model) {
        TranslatorUtils.from(dbObject, CODE_KEY, model.getCode());
        TranslatorUtils.from(dbObject, DISPLAY_KEY, model.getDisplay());
        TranslatorUtils.from(dbObject, DUBBING_KEY, model.getDubbing());

        return dbObject;
    }

    @Override
    public Language fromDBObject(DBObject dbObject, Language model) {
        model.setCode(TranslatorUtils.toString(dbObject, CODE_KEY));
        model.setDisplay(TranslatorUtils.toString(dbObject, DISPLAY_KEY));
        model.setDubbing(TranslatorUtils.toString(dbObject, DUBBING_KEY));

        return model;
    }
}
