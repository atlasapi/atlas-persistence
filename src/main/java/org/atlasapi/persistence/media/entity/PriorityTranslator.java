package org.atlasapi.persistence.media.entity;

import com.google.common.collect.Lists;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Priority;
import org.atlasapi.media.entity.PriorityScoreReasons;

import java.util.List;

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

            List<DBObject> positiveReasons = TranslatorUtils.toDBObjectList(priorityReasons, POSITIVE_REASONS_KEY);
            for (Object reason : positiveReasons) {
                if (reason != null && reason instanceof String) {
                    String string = (String) reason;
                    positiveScoreReasons.add(string);
                }
            }

            List<DBObject> negativeReasons = TranslatorUtils.toDBObjectList(priorityReasons, NEGATIVE_REASONS_KEY);
            for (Object reason : negativeReasons) {
                if (reason != null && reason instanceof String) {
                    String string = (String) reason;
                    negativeScoreReasons.add(string);
                }
            }
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
        BasicDBList positiveReasons = new BasicDBList();
        if (entity.getPriority().getReasons().getPositive() != null) {
            for (String reason : entity.getPriority().getReasons().getPositive()) {
                positiveReasons.add(reason);
            }
        }
        scoreReasons.put(POSITIVE_REASONS_KEY, positiveReasons);

        BasicDBList negativeReasons = new BasicDBList();
        if (entity.getPriority().getReasons().getNegative() != null) {
            for (String reason : entity.getPriority().getReasons().getNegative()) {
                negativeReasons.add(reason);
            }
        }
        scoreReasons.put(NEGATIVE_REASONS_KEY, negativeReasons);

        dbObject.put(REASONS_KEY, scoreReasons);
        return dbObject;
    }
}
