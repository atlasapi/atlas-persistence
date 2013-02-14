package org.atlasapi.media.channel;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class TemporalStringTranslator {
    private static final String START_DATE_KEY = "startDate";
    private static final String END_DATE_KEY = "endDate";
    private static final String VALUE_KEY = "value";
    
    public DBObject toDBObject(TemporalString temporalString) {
        DBObject dbo = new BasicDBObject();
        
        TranslatorUtils.fromLocalDate(dbo, START_DATE_KEY, temporalString.getStartDate());
        TranslatorUtils.fromLocalDate(dbo, END_DATE_KEY, temporalString.getEndDate());
        TranslatorUtils.from(dbo, VALUE_KEY, temporalString.getValue());
        
        return dbo;
    }
    
    public TemporalString fromDBObject(DBObject dbo) {
        return new TemporalString(
            TranslatorUtils.toString(dbo, VALUE_KEY),
            TranslatorUtils.toLocalDate(dbo, START_DATE_KEY),
            TranslatorUtils.toLocalDate(dbo, END_DATE_KEY));
    }
}
