package org.atlasapi.persistence.audit;

import static org.junit.Assert.assertEquals;

import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.testing.ComplexItemTestDataBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.time.TimeMachine;
import com.mongodb.DBObject;


public class PerHourAndDayMongoPersistenceAuditLogTest {

    private final TimeMachine clock = new TimeMachine();
    private DatabasedMongo mongo;
    private PerHourAndDayMongoPersistenceAuditLog auditLog;
    
    @Before
    public void setUp() {
        mongo = MongoTestHelper.anEmptyTestDatabase();
        auditLog = new PerHourAndDayMongoPersistenceAuditLog(mongo, clock);
    }
    
    @Test
    public void testWrites() {
        clock.jumpTo(new DateTime(2014, DateTimeConstants.JANUARY, 1, 3, 0, 0, 0));
        auditLog.logWrite(createItem(Publisher.BBC));
        auditLog.logWrite(createItem(Publisher.BBC));
        auditLog.logNoWrite(createItem(Publisher.BBC));
        auditLog.logNoWrite(createItem(Publisher.BBC));
        auditLog.logNoWrite(createItem(Publisher.BBC));
        auditLog.logNoWrite(createItem(Publisher.BBC));
        auditLog.logWrite(createBrand(Publisher.BBC));
        
        auditLog.logWrite(createItem(Publisher.METABROADCAST));
        auditLog.logNoWrite(createItem(Publisher.METABROADCAST));
        
        DBObject perDayBbc = Iterables.getOnlyElement(mongo.collection("perDayWriteLog").find(
                            new MongoQueryBuilder()
                                    .fieldEquals("publisher", Publisher.BBC.key().toLowerCase())
                                    .fieldEquals("timeBucket", "2014-01-01")
                                    .build()));
        
        assertBbcWrites(perDayBbc);
        
        DBObject perHour = Iterables.getOnlyElement(mongo.collection("perHourWriteLog").find(
                new MongoQueryBuilder()
                        .fieldEquals("publisher", Publisher.BBC.key().toLowerCase())
                        .fieldEquals("timeBucket", "2014-01-01T03")
                        .build()));
        
        assertBbcWrites(perHour);
        
        DBObject perDayMbst = Iterables.getOnlyElement(mongo.collection("perDayWriteLog").find(
                new MongoQueryBuilder()
                        .fieldEquals("publisher", Publisher.METABROADCAST.key().toLowerCase())
                        .fieldEquals("timeBucket", "2014-01-01")
                        .build()));

        assertMbstWrites(perDayMbst);

        DBObject perHourMbst = Iterables.getOnlyElement(mongo.collection("perHourWriteLog").find(
            new MongoQueryBuilder()
                    .fieldEquals("publisher", Publisher.METABROADCAST.key().toLowerCase())
                    .fieldEquals("timeBucket", "2014-01-01T03")
                    .build()));

        assertMbstWrites(perHourMbst);
        
    }
    
    private void assertBbcWrites(DBObject dbo) {
        DBObject noWrites = (DBObject) dbo.get("noWrite");
        //temporarily disabled
        //assertEquals(4, noWrites.get("item"));
        
        DBObject writes = (DBObject) dbo.get("write");
        assertEquals(2, writes.get("item"));
        assertEquals(1, writes.get("brand"));
        
    }
    
    private void assertMbstWrites(DBObject dbo) {
        //temporarily disabled
        //DBObject noWrites = (DBObject) dbo.get("noWrite");
        //assertEquals(1, noWrites.get("item"));
        
        DBObject writes = (DBObject) dbo.get("write");
        assertEquals(1, writes.get("item"));
    }
    
    private Item createItem(Publisher publisher) {
        Item item = ComplexItemTestDataBuilder.complexItem().build();
        item.setPublisher(publisher);
        return item;
    }
    
    private Brand createBrand(Publisher publisher) {
        return new Brand(null, null, publisher);
    }
    
}
