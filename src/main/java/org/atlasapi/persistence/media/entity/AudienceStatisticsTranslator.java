package org.atlasapi.persistence.media.entity;

import java.util.Comparator;

import org.atlasapi.media.entity.AudienceStatistics;
import org.atlasapi.media.entity.Demographic;

import com.google.common.base.Function;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;


public class AudienceStatisticsTranslator {

    private static final String TOTAL_VIEWERS_KEY = "totalViewers";
    private static final String VIEWING_SHARE_KEY = "viewingShare";
    private static final String DEMOGRAPHICS_KEY = "demographics";
    
    private static final Ordering<Demographic> DEMOGRAPHICS_ORDERING = 
            Ordering.from(new Comparator<Demographic>() {

                @Override
                public int compare(Demographic o1, Demographic o2) {
                    return ComparisonChain.start()
                            .compare(o1.getType(), o2.getType())
                            .result();
                }
            });
    
    private final DemographicTranslator demographicsTranslator;
    
    public AudienceStatisticsTranslator() {
        this.demographicsTranslator = new DemographicTranslator();
    }
    
    public DBObject toDBObject(DBObject dbObject, AudienceStatistics model) {
        TranslatorUtils.from(dbObject, TOTAL_VIEWERS_KEY, model.getTotalViewers());
        TranslatorUtils.from(dbObject, VIEWING_SHARE_KEY, model.getViewingShare());
        encodeDemographics(dbObject, model);
        return dbObject;
    }

    public AudienceStatistics fromDBObject(DBObject dbObject) {
        return new AudienceStatistics(
                TranslatorUtils.toLong(dbObject, TOTAL_VIEWERS_KEY),
                TranslatorUtils.toFloat(dbObject, VIEWING_SHARE_KEY),
                decodeDemographics(dbObject));
    }
    
    @SuppressWarnings("unchecked")
    private Iterable<Demographic> decodeDemographics(DBObject dbObject) {
        if (!dbObject.containsField(DEMOGRAPHICS_KEY)) {
            return ImmutableSet.of();
        }
        return Iterables.transform((Iterable<DBObject>) dbObject.get(DEMOGRAPHICS_KEY), 
            new Function<DBObject, Demographic>() {

                @Override
                public Demographic apply(DBObject input) {
                    return demographicsTranslator.fromDBObject(input);
                }
            }
        );
    }
    

    private void encodeDemographics(DBObject dbObject, AudienceStatistics model) {
        if (model.getDemographics().isEmpty()) {
            return;
        }
        
        TranslatorUtils.fromIterable(dbObject, 
            DEMOGRAPHICS_KEY, 
            DEMOGRAPHICS_ORDERING.sortedCopy(model.getDemographics()), 
            new Function<Demographic, DBObject>() {
    
                    @Override
                    public DBObject apply(Demographic demographic) {
                        return demographicsTranslator.toDBObject(new BasicDBObject(), demographic);
                    }
        
            });
    }
}
