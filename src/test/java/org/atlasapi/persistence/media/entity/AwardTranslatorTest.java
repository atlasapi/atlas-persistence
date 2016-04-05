package org.atlasapi.persistence.media.entity;

import static junit.framework.TestCase.assertEquals;

import org.atlasapi.media.entity.Award;
import org.junit.Test;

import com.mongodb.DBObject;

public class AwardTranslatorTest {

    private final AwardTranslator translator = new AwardTranslator();

    @Test
    public void awardTranslatorTest() {
        Award award = new Award();
        award.setOutcome("won");
        award.setDescription("Best Actor");
        award.setTitle("BAFTA");
        award.setYear(2009);

        DBObject dbObject = translator.toDBObject(award);

        assertEquals("won", dbObject.get(AwardTranslator.OUT_COME));
        assertEquals("Best Actor", dbObject.get(AwardTranslator.DESCRIPTION));
        assertEquals("BAFTA", dbObject.get(AwardTranslator.TITLE));
        assertEquals(2009, dbObject.get(AwardTranslator.YEAR));
    }
}
