package org.atlasapi.persistence.media.entity;

import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.mongodb.DBObject;
import junit.framework.TestCase;
import org.atlasapi.media.entity.Policy;
import org.junit.Test;

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
}