package org.atlasapi.persistence.ids;

import static org.junit.Assert.*;

import java.math.BigInteger;

import org.junit.Test;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;

public class MongoSequentialIdGeneratorTest {

    private final DatabasedMongo mongo = MongoTestHelper.anEmptyTestDatabase();
    
    @Test
    public void testGenerate() {
        MongoSequentialIdGenerator generator = new MongoSequentialIdGenerator(mongo, "one");

        DBCollection collection = mongo.collection("id");
        SubstitutionTableNumberCodec codec = new SubstitutionTableNumberCodec();
        
        long expectedStart = new Double(Math.pow(codec.getAlphabet().size(), 3)).longValue();
        
        assertEquals(expectedStart, collection.findOne("one").get("nextId"));
        
        String generated = generator.generate();
        assertEquals(codec.encode(BigInteger.valueOf(expectedStart)), generated);
        
        
        generated = generator.generate();
        assertEquals(codec.encode(BigInteger.valueOf(expectedStart+1)), generated);

        assertEquals(expectedStart+2, collection.findOne("one").get("nextId"));
    }

}
