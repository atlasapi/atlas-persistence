package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.DemographicSegment;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class DemographicSegmentTranslator {

    private static final String KEY_KEY = "key";
    private static final String LABEL_KEY = "label";
    private static final String VALUE_KEY = "value";
    
    public DemographicSegment fromDBObject(DBObject input) {
        return new DemographicSegment(
                TranslatorUtils.toString(input, KEY_KEY),
                TranslatorUtils.toString(input, LABEL_KEY),
                TranslatorUtils.toFloat(input, VALUE_KEY));
    }

    public DBObject toDBObject(BasicDBObject dbObject, DemographicSegment segment) {
        TranslatorUtils.from(dbObject, KEY_KEY, segment.getKey());
        TranslatorUtils.from(dbObject, LABEL_KEY, segment.getLabel());
        TranslatorUtils.from(dbObject, VALUE_KEY, segment.getValue());
        return dbObject;
    }

}
