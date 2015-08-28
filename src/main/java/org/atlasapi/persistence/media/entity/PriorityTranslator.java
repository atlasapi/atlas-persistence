package org.atlasapi.persistence.media.entity;

import com.google.common.collect.Lists;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
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
        List<String> scoreReasons = Lists.newArrayList();
        try {
            score = TranslatorUtils.toDouble(dbObject, ITEM_PRIORITY_KEY);
        } catch (ClassCastException e) {
            DBObject priority = TranslatorUtils.toDBObject(dbObject, ITEM_PRIORITY_KEY);
            score = TranslatorUtils.toDouble(priority, SCORE_KEY);
            List<DBObject> reasons = TranslatorUtils.toDBObjectList(priority, REASONS_KEY);
            for (Object reason : reasons) {
                if (reason != null && reason instanceof String) {
                    String string = (String) reason;
                    scoreReasons.add(string);
                }
            }
        }
        return new Priority(score, scoreReasons);
    }

    public void setPriority(DBObject dbObject, Described entity) {
        TranslatorUtils.from(dbObject, ITEM_PRIORITY_KEY, getPriorityDBObject(entity));
    }

    public DBObject getPriorityDBObject(Described entity) {
        DBObject dbObject = new BasicDBObject();
        TranslatorUtils.from(dbObject, SCORE_KEY, entity.getPriority().getScore());

        BasicDBList list = new BasicDBList();
        if (entity.getPriority().getReasons() != null) {
            for (String reason : entity.getPriority().getReasons()) {
                list.add(reason);
            }
        }
        dbObject.put(REASONS_KEY, list);
        return dbObject;
    }
}
