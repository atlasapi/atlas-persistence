package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Review;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;


public class ReviewTranslator {

    private static final String LOCALE_KEY = "locale";
    private static final String REVIEW_KEY = "review";
    
    public DBObject toDBObject(DBObject dbObject, Review model) {
        TranslatorUtils.fromLocaleToLanguageTag(dbObject, LOCALE_KEY, model.getLocale());
        TranslatorUtils.from(dbObject, REVIEW_KEY, model.getReview());
        return dbObject;
    }

    public Review fromDBObject(DBObject dbObject) {
        return new Review(TranslatorUtils.toLocaleFromLanguageTag(dbObject, LOCALE_KEY),
                          TranslatorUtils.toString(dbObject, REVIEW_KEY));
    }

}
