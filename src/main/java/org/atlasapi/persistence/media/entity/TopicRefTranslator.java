package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.TopicRef;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class TopicRefTranslator {

	private static final String TOPIC_KEY = "topic";
	private static final String WEIGHTING_KEY = "weighting";
	private static final String SUPERVISED_KEY = "supervised";
	private static final String RELATIONSHIP_KEY = "relationship";
	private static final String OFFSET_KEY = "offset";

	public DBObject toDBObject(TopicRef contentTopic) {
		  DBObject dbo = new BasicDBObject();
		  TranslatorUtils.from(dbo, TOPIC_KEY, contentTopic.getTopic());
		  TranslatorUtils.from(dbo, WEIGHTING_KEY, (Float)contentTopic.getWeighting());
		  TranslatorUtils.from(dbo, SUPERVISED_KEY, contentTopic.isSupervised());
		  TranslatorUtils.from(dbo, RELATIONSHIP_KEY, contentTopic.getRelationship().name());
		  TranslatorUtils.from(dbo, OFFSET_KEY, contentTopic.getOffset());
		  
		  return dbo;
	}
	
	public TopicRef fromDBObject(DBObject dbo) {
		return new TopicRef(TranslatorUtils.toLong(dbo, TOPIC_KEY), 
				TranslatorUtils.toFloat(dbo, WEIGHTING_KEY),
				TranslatorUtils.toBoolean(dbo, SUPERVISED_KEY),
                TopicRef.Relationship.valueOf(TranslatorUtils.toString(dbo, RELATIONSHIP_KEY)),
                TranslatorUtils.toInteger(dbo, OFFSET_KEY));
	}

}
