package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.Demographic;
import org.atlasapi.media.entity.DemographicSegment;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class DemographicTranslator {

    private static final String TYPE_KEY = "type";
    private static final String SEGMENTS_KEY = "segments";
    
    private final DemographicSegmentTranslator demographicSegmentTranslator;
    
    public DemographicTranslator() {
        this.demographicSegmentTranslator = new DemographicSegmentTranslator();
    }
    
    public DBObject toDBObject(BasicDBObject dbObject, Demographic demographic) {
        TranslatorUtils.from(dbObject, TYPE_KEY, demographic.getType());
        encodeDemographicSegments(dbObject, demographic);
        return dbObject;
    }

    public Demographic fromDBObject(DBObject input) {
        return new Demographic(
                TranslatorUtils.toString(input, TYPE_KEY),
                decodeDemographicSegments(input));
    }
    
    @SuppressWarnings("unchecked")
    private Iterable<DemographicSegment> decodeDemographicSegments(DBObject dbObject) {
        if (!dbObject.containsField(SEGMENTS_KEY)) {
            return ImmutableSet.of();
        }
        return Iterables.transform((Iterable<DBObject>) dbObject.get(SEGMENTS_KEY), 
            new Function<DBObject, DemographicSegment>() {

                @Override
                public DemographicSegment apply(DBObject input) {
                    return demographicSegmentTranslator.fromDBObject(input);
                }
            }
        );
    }
    
    private void encodeDemographicSegments(DBObject dbObject, Demographic model) {
        if (model.getSegments().isEmpty()) {
            return;
        }
        
        TranslatorUtils.fromIterable(dbObject, 
            SEGMENTS_KEY, 
            model.getSegments(), 
            new Function<DemographicSegment, DBObject>() {
    
                    @Override
                    public DBObject apply(DemographicSegment demographic) {
                        return demographicSegmentTranslator.toDBObject(new BasicDBObject(), demographic);
                    }
        
            });
    }

}
