package org.atlasapi.persistence.media.entity;

import java.util.Date;

import org.atlasapi.media.entity.Distribution;

import com.metabroadcast.common.time.DateTimeZones;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DistributionTranslatorTest {

    protected static final String FORMAT = "format";
    protected static final String RELEASE_DATE = "releaseDate";
    protected static final String DISTRIBUTOR = "distributor";
    DistributionTranslator distributionTranslator;

    public DistributionTranslatorTest() {
        distributionTranslator = new DistributionTranslator();
    }

    @Test
    public void translationOfDistributionToDatabaseObject() {
        Distribution distribution = makeDistribution();
        DBObject dbo = new BasicDBObject();
        distributionTranslator.toDBObject(dbo, distribution);

        assertTrue(dbo.containsField(FORMAT));
        assertTrue(dbo.containsField(RELEASE_DATE));
        assertTrue(dbo.containsField(DISTRIBUTOR));

        assertEquals(distribution.getDistributor(), dbo.get(DISTRIBUTOR));
        assertEquals(distribution.getFormat(), dbo.get(FORMAT));
        assertEquals(distribution.getReleaseDate().toDate(), dbo.get(RELEASE_DATE));
    }

    @Test
    public void translationOfDistributionFromDatabaseObject() {
        Distribution distribution = makeDistribution();
        DBObject dbo = new BasicDBObject();
        distributionTranslator.toDBObject(dbo, distribution);

        Distribution output = distributionTranslator.fromDBObject(dbo);

        assertEquals(distribution.getReleaseDate(), output.getReleaseDate());
        assertEquals(distribution.getFormat(), output.getFormat());
        assertEquals(distribution.getDistributor(), output.getDistributor());
    }

    public Distribution makeDistribution() {
        return Distribution.builder()
                .withDistributor("distributor")
                .withFormat("format")
                .withReleaseDate(new DateTime(DateTimeZone.UTC))
                .build();

    }
}