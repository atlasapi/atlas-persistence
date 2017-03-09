package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Review;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.ReviewType;
import org.joda.time.DateTime;

public class ReviewTranslator {

    private static final String LOCALE_KEY = "locale";
    private static final String REVIEW_KEY = "review";
    private static final String AUTHOR_KEY = "author";
    private static final String AUTHOR_INITIALS_KEY = "author_initials";
    private static final String RATING_KEY = "rating";
    private static final String DATE_KEY = "date";
    private static final String REVIEW_TYPE_KEY = "review_type";
    private static final String PUBLISHER_KEY = "publisher_key";
    
    public DBObject toDBObject(DBObject dbObject, Review model) {
        TranslatorUtils.fromLocaleToLanguageTag(dbObject, LOCALE_KEY, model.getLocale());
        TranslatorUtils.from(dbObject, REVIEW_KEY, model.getReview());
        TranslatorUtils.from(dbObject, PUBLISHER_KEY, model.getPublisherKey());
        TranslatorUtils.from(dbObject, REVIEW_TYPE_KEY, model.getReviewTypeKey());
        TranslatorUtils.from(dbObject, AUTHOR_KEY, model.getAuthor());
        TranslatorUtils.from(dbObject, AUTHOR_INITIALS_KEY, model.getAuthorInitials());
        TranslatorUtils.from(dbObject, RATING_KEY, model.getRating());
        TranslatorUtils.from(dbObject, DATE_KEY, model.getDate());
        return dbObject;
    }

    public Review fromDBObject(DBObject dbObject) {

        DateTime dateTime = TranslatorUtils.toDateTime(dbObject, DATE_KEY);

        return Review.builder()
                .withLocale(TranslatorUtils.toLocaleFromLanguageTag(dbObject, LOCALE_KEY))
                .withReview(TranslatorUtils.toString(dbObject, REVIEW_KEY))
                .withPublisherKey(TranslatorUtils.toString(dbObject, PUBLISHER_KEY))
                .withReviewTypeKey(TranslatorUtils.toString(dbObject, REVIEW_TYPE_KEY))
                .withAuthor(TranslatorUtils.toString(dbObject, AUTHOR_KEY))
                .withAuthorInitials(TranslatorUtils.toString(dbObject, AUTHOR_INITIALS_KEY))
                .withRating(TranslatorUtils.toString(dbObject, RATING_KEY))
                .withDate(dateTime != null ? dateTime.toDate() : null)
                .build();
    }

}
