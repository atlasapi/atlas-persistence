package org.atlasapi.persistence.media.entity;

import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Priority;

import java.util.List;

public class PriorityTranslator {

    private static final String SCORE_KEY = "score";
    private static final String REASON_KEY = "reason";
    private static final String REASONS_KEY = "reasons";

    public Priority getPriority(DBObject dbObject) {
        Double score = TranslatorUtils.toDouble(dbObject, SCORE_KEY);
        List<String> priorityScoreReasons = null;
        if (dbObject.containsField(REASONS_KEY)) {
            List<DBObject> dbos = TranslatorUtils.toDBObjectList(dbObject, REASONS_KEY);
            for (DBObject object : dbos) {
                priorityScoreReasons.add(TranslatorUtils.toString(object, REASON_KEY));
            }
        }
        return new Priority(score, priorityScoreReasons);
    }

    public void setPriority(DBObject dbObject, Described entity) {
        TranslatorUtils.from(dbObject, SCORE_KEY, entity.getPriority().getScore());
        BasicDBList list = new BasicDBList();
        list.addAll(ImmutableSet.copyOf(entity.getPriority().getReasons()));
        TranslatorUtils.from(dbObject, REASONS_KEY, list);
    }
}
