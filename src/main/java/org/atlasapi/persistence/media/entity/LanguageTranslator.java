package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Language;
import org.atlasapi.persistence.ModelTranslator;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;

import com.mongodb.DBObject;

public class LanguageTranslator {

    protected static final String CODE_KEY = "code";
    protected static final String DISPLAY_KEY = "display";
    protected static final String DUBBING_KEY = "dubbing";

    public DBObject toDBObject(DBObject dbObject, Language model) {
        TranslatorUtils.from(dbObject, CODE_KEY, model.getCode());
        TranslatorUtils.from(dbObject, DISPLAY_KEY, model.getDisplay());
        TranslatorUtils.from(dbObject, DUBBING_KEY, model.getDubbing());

        return dbObject;
    }

    public Language fromDBObject(DBObject dbObject) {
        return Language.builder()
                .withCode(TranslatorUtils.toString(dbObject, CODE_KEY))
                .withDisplay(TranslatorUtils.toString(dbObject, DISPLAY_KEY))
                .withDubbing(TranslatorUtils.toString(dbObject, DUBBING_KEY))
                .build();
    }
}
