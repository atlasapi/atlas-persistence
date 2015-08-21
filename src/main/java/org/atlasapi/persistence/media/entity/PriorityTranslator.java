package org.atlasapi.persistence.media.entity;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
    private static final String REASON_KEY = "reason";
    private static final String REASONS_KEY = "reasons";

    public Priority getPriority(DBObject dbObject) {
        Double score = TranslatorUtils.toDouble(dbObject, SCORE_KEY);
        List<PriorityScoreReasons> priorityScoreReasons = null;
        if (dbObject.containsField(REASONS_KEY)) {
            List<DBObject> dbos = TranslatorUtils.toDBObjectList(dbObject, REASONS_KEY);
            for (DBObject object : dbos) {
                priorityScoreReasons.add(new PriorityScoreReasons(TranslatorUtils.toString(object, REASON_KEY)));
            }
        }
        return new Priority(score, priorityScoreReasons);
    }

    public void setPriority(DBObject dbObject, Described entity) {
        TranslatorUtils.from(dbObject, SCORE_KEY, entity.getPriority().getScore());
        BasicDBList list = new BasicDBList();
        list.addAll(ImmutableSet.copyOf(Iterables.transform(entity.getPriority().getReasons(), FROM_REASONS)));
        TranslatorUtils.from(dbObject, REASONS_KEY, list);
    }

    private Function<PriorityScoreReasons, DBObject> FROM_REASONS = new Function<PriorityScoreReasons, DBObject>() {
        @Override
        public DBObject apply(PriorityScoreReasons reasons) {
            return toDBObject(reasons);
        }
    };

    public DBObject toDBObject(PriorityScoreReasons reason) {
        DBObject dbo = new BasicDBObject();
        TranslatorUtils.from(dbo, REASON_KEY, reason.getReason());
        return dbo;
    }
}
