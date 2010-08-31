package org.atlasapi.persistence;

import org.atlasapi.persistence.content.AggregateContentListener;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.EventFiringContentWriter;
import org.atlasapi.persistence.content.mongo.MongoDbBackedContentBootstrapper;
import org.atlasapi.persistence.content.mongo.MongoDbBackedContentStore;
import org.atlasapi.persistence.content.mongo.MongoRoughSearch;
import org.atlasapi.persistence.tracking.ContentMentionStore;
import org.atlasapi.persistence.tracking.MongoDBBackedContentMentionStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.mongodb.Mongo;

@Configuration
public class MongoContentPersistenceModule implements ContentPersistenceModule {

	private static final String DB_NAME = "atlas";
	
	private @Autowired Mongo mongo;
	
	public @Bean ContentMentionStore contentMentionStore() {
		return new MongoDBBackedContentMentionStore(mongo, DB_NAME);
	}
	
	public @Bean ContentWriter persistentWriter() {
		return new EventFiringContentWriter(contentStore(), contentStore(), contentListener());
	}	
	
	public @Bean MongoRoughSearch roughSearch() {
		return new MongoRoughSearch(contentStore());
	}
	
	public @Bean(name={"mongoContentStore", "aliasWriter"}) MongoDbBackedContentStore contentStore() {
		return new MongoDbBackedContentStore(mongo, DB_NAME);
	}
	
	@Bean MongoDbBackedContentBootstrapper contentBootstrapper() {
		return new MongoDbBackedContentBootstrapper(contentListener(), contentStore());
	}

	public @Bean AggregateContentListener contentListener() {
		return new AggregateContentListener();
	}
}
