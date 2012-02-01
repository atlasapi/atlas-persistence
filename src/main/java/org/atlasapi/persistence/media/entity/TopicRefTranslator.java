package org.atlasapi.persistence.media.entity;

import org.atlasapi.media.entity.TopicRef;

import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class TopicRefTranslator {

	private static final String TOPIC_KEY = "topic";
	private static final String WEIGHTING_KEY = "weighting";
	private static final String SUPERVISED_KEY = "supervised";

	public DBObject toDBObject(TopicRef contentTopic) {
		  DBObject dbo = new BasicDBObject();
		  TranslatorUtils.from(dbo, TOPIC_KEY, contentTopic.getTopic());
		  TranslatorUtils.from(dbo, WEIGHTING_KEY, (Float)contentTopic.getWeighting());
		  TranslatorUtils.from(dbo, SUPERVISED_KEY, contentTopic.isSupervised());
		  
		  return dbo;
	}
	
	public TopicRef fromDBObject(DBObject dbo) {
		return new TopicRef(TranslatorUtils.toString(dbo, TOPIC_KEY), 
				TranslatorUtils.toFloat(dbo, WEIGHTING_KEY),
				TranslatorUtils.toBoolean(dbo, SUPERVISED_KEY));
	}

}
