package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.common.Id;
import org.atlasapi.media.entity.TopicRef;
import org.junit.Test;

import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.atlasapi.media.entity.ContentGroupRef;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ContentGroupRefTranslatorTest {

	@Test
	public void encodeAndDecodeContentGroupRef() {
		
        DBCollection collection = MongoTestHelper.anEmptyTestDatabase().collection("test");
 
        ContentGroupRefTranslator translator = new ContentGroupRefTranslator();
        
        ContentGroupRef contentGroupRef = new ContentGroupRef(Id.valueOf(1L), "uri");
        DBObject dbObject = translator.toDBObject(contentGroupRef);
        dbObject.put(MongoConstants.ID, "test");
		collection.save(dbObject);
        
        ContentGroupRef queried = translator.fromDBObject(collection.findOne(new MongoQueryBuilder().idEquals("test").build()));
        
        assertThat(contentGroupRef.getId(), is(equalTo(queried.getId())));
        assertThat(contentGroupRef.getUri(), is(equalTo(queried.getUri())));
	}
}
