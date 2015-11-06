package org.atlasapi.persistence.media.entity;

import com.google.common.collect.Lists;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Priority;
import org.atlasapi.media.entity.PriorityScoreReasons;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class PriorityTranslator {

    private static final String SCORE_KEY = "score";
    private static final String REASONS_KEY = "reasons";
    private static final String POSITIVE_REASONS_KEY = "positive";
    private static final String NEGATIVE_REASONS_KEY = "negative";
    private static final String ITEM_PRIORITY_KEY = "priority";

    public Priority getPriority(DBObject dbObject) {
        Double score = null;
        List<String> positiveScoreReasons = Lists.newArrayList();
        List<String> negativeScoreReasons = Lists.newArrayList();
        try {
            score = TranslatorUtils.toDouble(dbObject, ITEM_PRIORITY_KEY);
        } catch (ClassCastException e) {
            DBObject priority = TranslatorUtils.toDBObject(dbObject, ITEM_PRIORITY_KEY);
            score = TranslatorUtils.toDouble(priority, SCORE_KEY);
            DBObject priorityReasons = TranslatorUtils.toDBObject(priority, REASONS_KEY);

            positiveScoreReasons = getPositivePriorityScoreReasonsFromDB(priorityReasons);
            negativeScoreReasons = getNegativePriorityScoreReasonsFromDB(priorityReasons);
        }

        PriorityScoreReasons priorityScoreReasons = new PriorityScoreReasons(positiveScoreReasons, negativeScoreReasons);
        return new Priority(score, priorityScoreReasons);
    }

    public void setPriority(DBObject dbObject, Described entity) {
        TranslatorUtils.from(dbObject, ITEM_PRIORITY_KEY, getPriorityDBObject(entity));
    }

    public DBObject getPriorityDBObject(Described entity) {
        DBObject dbObject = new BasicDBObject();
        TranslatorUtils.from(dbObject, SCORE_KEY, entity.getPriority().getScore());
        DBObject scoreReasons = new BasicDBObject();
        TranslatorUtils.fromIterable(scoreReasons, entity.getPriority().getReasons().getPositive(),
                POSITIVE_REASONS_KEY);
        TranslatorUtils.fromIterable(scoreReasons, entity.getPriority().getReasons().getNegative(),
                NEGATIVE_REASONS_KEY);
        dbObject.put(REASONS_KEY, scoreReasons);
        return dbObject;
    }

    public List<String> getPositivePriorityScoreReasonsFromDB(DBObject priorityReasons) {
        List<String> positiveScoreReasons = Lists.newArrayList();
        Optional<List<String>> positiveReasons = Optional.of(TranslatorUtils
                .toList(priorityReasons, POSITIVE_REASONS_KEY));

        if (positiveReasons.isPresent()) {
            Iterator<String> positiveReasonsIterator = positiveReasons.get().iterator();
            while (positiveReasonsIterator.hasNext()) {
                positiveScoreReasons.add(positiveReasonsIterator.next());
            }
            return positiveScoreReasons;
        } else {
            return positiveScoreReasons;
        }
    }

    public List<String> getNegativePriorityScoreReasonsFromDB(DBObject priorityReasons) {
        List<String> negativeScoreReasons = Lists.newArrayList();
        Optional<List<String>> negativeReasons = Optional.of(TranslatorUtils
                .toList(priorityReasons, NEGATIVE_REASONS_KEY));

        if (negativeReasons.isPresent()) {
            Iterator<String> negativeReasonsIterator = negativeReasons.get().iterator();
            while (negativeReasonsIterator.hasNext()) {
                negativeScoreReasons.add(negativeReasonsIterator.next());
            }
            return negativeScoreReasons;
        } else {
            return negativeScoreReasons;
        }
    }
}
