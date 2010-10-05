package org.atlasapi.persistence;

import org.atlasapi.persistence.content.AggregateContentListener;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.DefinitiveContentWriter;
import org.atlasapi.persistence.content.EventFiringContentWriter;
import org.atlasapi.persistence.content.mongo.MongoDbBackedContentBootstrapper;
import org.atlasapi.persistence.content.mongo.MongoDbBackedContentStore;
import org.atlasapi.persistence.content.mongo.MongoRoughSearch;
import org.atlasapi.persistence.tracking.ContentMentionStore;
import org.atlasapi.persistence.tracking.MongoDBBackedContentMentionStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

@Configuration
public class MongoContentPersistenceModule implements ContentPersistenceModule {

	private @Autowired DatabasedMongo db;
	
	public @Bean ContentMentionStore contentMentionStore() {
		return new MongoDBBackedContentMentionStore(db);
	}
	
	public @Bean ContentWriter persistentWriter() {
		return eventFiringContentWriter();
	}
	
	public @Bean DefinitiveContentWriter definitiveWriter() {
	    return eventFiringContentWriter();
	}
	
	public @Bean MongoRoughSearch roughSearch() {
		return new MongoRoughSearch(contentStore());
	}
	
	public @Bean(name={"mongoContentStore", "aliasWriter"}) MongoDbBackedContentStore contentStore() {
		return new MongoDbBackedContentStore(db);
	}
	
	@Bean EventFiringContentWriter eventFiringContentWriter() {
	    return new EventFiringContentWriter(contentStore(), contentStore(), contentListener());
	}
	
	@Bean MongoDbBackedContentBootstrapper contentBootstrapper() {
		return new MongoDbBackedContentBootstrapper(contentListener(), contentStore());
	}

	public @Bean AggregateContentListener contentListener() {
		return new AggregateContentListener();
	}
}
