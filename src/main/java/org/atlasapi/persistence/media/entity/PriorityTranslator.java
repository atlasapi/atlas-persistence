package org.atlasapi.persistence.media.entity;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Priority;

import java.util.List;

public class PriorityTranslator {

    private static final String SCORE_KEY = "score";
    private static final String REASONS_KEY = "reasons";
    private static final String ITEM_PRIORITY_KEY = "priority";

    public Priority getPriority(DBObject dbObject) {
        Double score = null;
        List<String> priorityScoreReasons = Lists.newArrayList();
        if(dbObject.containsField(ITEM_PRIORITY_KEY)) {
            score = Double.parseDouble(TranslatorUtils.toString(dbObject, ITEM_PRIORITY_KEY));
        } else {
            if (dbObject.containsField(SCORE_KEY)) {
                score = TranslatorUtils.toDouble(dbObject, SCORE_KEY);
            }
            if (dbObject.containsField(REASONS_KEY)) {
                priorityScoreReasons = TranslatorUtils.toList(dbObject, REASONS_KEY);
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
