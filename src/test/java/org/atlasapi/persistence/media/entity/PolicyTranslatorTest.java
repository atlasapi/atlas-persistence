package org.atlasapi.persistence.media.entity;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

import junit.framework.TestCase;

import org.atlasapi.media.entity.Policy;
import org.atlasapi.media.entity.simple.Pricing;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import java.util.Currency;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PolicyTranslatorTest extends TestCase {

    private final PolicyTranslator translator = new PolicyTranslator();

    @Test
    public void testTranslateFromDboContentWithNullTermsOfUse() {
        DBObject dbObject = mock(DBObject.class);
        when(dbObject.get(MongoConstants.ID)).thenReturn("1");
        when(dbObject.get("termsOfUse")).thenReturn(null);
        Policy policy = translator.fromDBObject(dbObject, new Policy());

        assertTrue(policy != null);
    }

    @Test
    public void testTranslateFromDboContentWithTermsOfUse() {
        DBObject dbObject = mock(DBObject.class);

        when(dbObject.get(MongoConstants.ID)).thenReturn("1");
        when(dbObject.containsField("termsOfUse")).thenReturn(true);
        when(dbObject.get("termsOfUse")).thenReturn("ToU text");

        Policy policy = translator.fromDBObject(dbObject, new Policy());

        assertThat(policy.getTermsOfUse(), is("ToU text"));
    }

    @Test
    public void testToAndFromDBObjectWithPricing() {
        Policy policy = new Policy();
        Price price1 = new Price(Currency.getInstance("GBP"), 1);
        Price price2 = new Price(Currency.getInstance("GBP"), 2);
        DateTime startTime1 = DateTime.now(DateTimeZone.UTC);
        DateTime endTime1 = DateTime.now(DateTimeZone.UTC).plusHours(1);
        DateTime startTime2 = DateTime.now(DateTimeZone.UTC).plusHours(1);
        DateTime endTime2 = DateTime.now(DateTimeZone.UTC).plusHours(2);
        Pricing pricing1 = new Pricing(startTime1, endTime1, price1);
        Pricing pricing2 = new Pricing(startTime2, endTime2, price2);
        policy.setPricing(ImmutableList.of(pricing1, pricing2));


        Policy result = toDbObjectAndBack(policy);

        assertThat(result.getPricing().get(0).getPrice(), is(price1));
        assertThat(result.getPricing().get(0).getStartTime(), is(startTime1));
        assertThat(result.getPricing().get(0).getEndTime(), is(endTime1));

        assertThat(result.getPricing().get(1).getPrice(), is(price2));
        assertThat(result.getPricing().get(1).getStartTime(), is(startTime2));
        assertThat(result.getPricing().get(1).getEndTime(), is(endTime2));

    }

    private Policy toDbObjectAndBack(Policy policy) {
        return translator.fromDBObject(translator.toDBObject(new BasicDBObject(), policy), new Policy());
    }
    
    @Test
    public void testToAndFromDbObjectWithSubscriptionPackages() {
        Set<String> packages = ImmutableSet.of("a", "b");
        Policy policy = new Policy();
        policy.setSubscriptionPackages(packages);
        
        Policy result = toDbObjectAndBack(policy);
        assertThat(result.getSubscriptionPackages(), is(packages));
    }
}