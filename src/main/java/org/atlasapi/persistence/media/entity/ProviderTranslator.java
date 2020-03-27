package org.atlasapi.persistence.media.entity;

import com.metabroadcast.common.persistence.translator.ModelTranslator;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;

import org.atlasapi.media.entity.Provider;

import com.mongodb.DBObject;

public class ProviderTranslator {

    private static String NAME_KEY = "name";
    private static String ICON_URL_KEY = "iconUrl";

    public Provider fromDBObject(DBObject dbObject) {
        return new Provider(
                TranslatorUtils.toString(dbObject, NAME_KEY),
                TranslatorUtils.toString(dbObject, ICON_URL_KEY)
        );
    }

    public DBObject toDBObject(DBObject dbObject, Provider entity) {
        TranslatorUtils.from(dbObject, NAME_KEY, entity.getName());
        TranslatorUtils.from(dbObject, ICON_URL_KEY, entity.getIconUrl());

        return dbObject;
    }
}
