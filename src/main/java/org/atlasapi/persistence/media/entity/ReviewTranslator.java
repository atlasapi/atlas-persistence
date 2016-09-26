package org.atlasapi.persistence.media.entity;

import java.util.List;

import org.atlasapi.media.entity.LocalizedTitle;
import org.atlasapi.media.entity.Review;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.mongodb.DBObject;


public class ReviewTranslator {

    private static final String LOCALE_KEY = "locale";
    private static final String REVIEW_KEY = "review";
    private static final String TYPE_KEY = "type";
    private static final String PEOPLE_KEY = "people";
    
    public DBObject toDBObject(DBObject dbObject, Review model) {
        TranslatorUtils.fromLocaleToLanguageTag(dbObject, LOCALE_KEY, model.getLocale());
        TranslatorUtils.from(dbObject, REVIEW_KEY, model.getReview());
        TranslatorUtils.from(dbObject, TYPE_KEY, model.getType());
        TranslatorUtils.from(dbObject, PEOPLE_KEY, model.getPeople());
        return dbObject;
    }

    public Review fromDBObject(DBObject dbObject) {
        Review review = new Review(TranslatorUtils.toLocaleFromLanguageTag(dbObject, LOCALE_KEY),
                TranslatorUtils.toString(dbObject, REVIEW_KEY));
        review.setType(TranslatorUtils.toString(dbObject, TYPE_KEY));
        // TODO: List<CrewMember> needs to be set here
        return review;
    }

}
