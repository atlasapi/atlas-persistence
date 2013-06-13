package org.atlasapi.media.channel;

import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class TemporalTitleTranslator {
    private static final String START_DATE_KEY = "startDate";
    private static final String END_DATE_KEY = "endDate";
    private static final String TITLE_KEY = "value";
    
    public DBObject toDBObject(TemporalField<String> temporalTitle) {
        DBObject dbo = new BasicDBObject();
        
        TranslatorUtils.fromLocalDate(dbo, START_DATE_KEY, temporalTitle.getStartDate());
        TranslatorUtils.fromLocalDate(dbo, END_DATE_KEY, temporalTitle.getEndDate());
        TranslatorUtils.from(dbo, TITLE_KEY, temporalTitle.getValue());
        
        return dbo;
    }
    
    public TemporalField<String> fromDBObject(DBObject dbo) {
        return new TemporalField<String>(
            TranslatorUtils.toString(dbo, TITLE_KEY),
            TranslatorUtils.toLocalDate(dbo, START_DATE_KEY),
            TranslatorUtils.toLocalDate(dbo, END_DATE_KEY));
    }

    
    public void fromTemporalTitleSet(DBObject dbObject, String key, Iterable<TemporalField<String>> titles) {
        BasicDBList values = new BasicDBList();
        for (TemporalField<String> value : titles) {
            if (value != null) {
                values.add(toDBObject(value));
            }
        }
        dbObject.put(key, values);
    }
    
    @SuppressWarnings("unchecked")
    public Set<TemporalField<String>> toTemporalTitleSet(DBObject object, String name) {
        if (object.containsField(name)) {
            Set<TemporalField<String>> temporalString = Sets.newLinkedHashSet();
            for (DBObject element : (List<DBObject>) object.get(name)) {
                temporalString.add(fromDBObject(element));
            }
            return temporalString;
        }
        return Sets.newLinkedHashSet();
    }
}
