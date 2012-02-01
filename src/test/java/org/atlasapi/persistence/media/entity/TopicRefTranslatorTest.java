package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.TopicRef;
import org.junit.Test;

import com.metabroadcast.common.persistence.MongoTestHelper;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TopicRefTranslatorTest {

	@Test
	public void encodeAndDecodeTopicRef() {
		
	   	MongoTestHelper.ensureMongoIsRunning();
        DBCollection collection = MongoTestHelper.anEmptyTestDatabase().collection("test");
 
        TopicRefTranslator translator = new TopicRefTranslator();
        
        TopicRef topicRef = new TopicRef("http://foo/bar", 0.01f, true);
        DBObject dbObject = translator.toDBObject(topicRef);
        dbObject.put(MongoConstants.ID, "test");
		collection.save(dbObject);
        
        TopicRef queried = translator.fromDBObject(collection.findOne(new MongoQueryBuilder().idEquals("test").build()));
        
        assertThat(topicRef.getTopic(), is(equalTo(queried.getTopic())));
        assertThat(topicRef.getWeighting(), is(equalTo(queried.getWeighting())));
        assertThat(topicRef.isSupervised(), is(equalTo(queried.isSupervised())));
	}
}
