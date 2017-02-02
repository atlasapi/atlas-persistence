package org.atlasapi.persistence.media.entity;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.atlasapi.media.entity.CrewMember;
import org.atlasapi.media.entity.Distribution;
import org.atlasapi.media.entity.LocalizedTitle;
import org.atlasapi.media.entity.Person;
import org.atlasapi.media.entity.Review;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;


public class ReviewTranslator {

    private static final String LOCALE_KEY = "locale";
    private static final String REVIEW_KEY = "review";
    private static final String TYPE_KEY = "type";
    private final AuthorTranslator authorTranslator = AuthorTranslator.create();

    public ReviewTranslator() {

    }
    
    public DBObject toDBObject(DBObject dbObject, Review model) {
        TranslatorUtils.fromLocaleToLanguageTag(dbObject, LOCALE_KEY, model.getLocale());
        TranslatorUtils.from(dbObject, REVIEW_KEY, model.getReview());
        TranslatorUtils.from(dbObject, TYPE_KEY, model.getType());
        authorTranslator.toDBObject(dbObject, model.getAuthor());

        return dbObject;
    }

    public Review fromDBObject(DBObject dbObject) {
        Review review = new Review(TranslatorUtils.toLocaleFromLanguageTag(dbObject, LOCALE_KEY),
                TranslatorUtils.toString(dbObject, REVIEW_KEY));
        review.setType(TranslatorUtils.toString(dbObject, TYPE_KEY));
        review.setAuthor(authorTranslator.fromDBObject(dbObject));

        return review;
    }

}
