package org.uriplay.persistence;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.uriplay.persistence.content.AggregateContentListener;
import org.uriplay.persistence.content.ContentListener;
import org.uriplay.persistence.content.ContentWriter;
import org.uriplay.persistence.content.EventFiringContentWriter;
import org.uriplay.persistence.content.MongoDbBackedContentBootstrapper;
import org.uriplay.persistence.content.QueueingContentListener;
import org.uriplay.persistence.content.mongo.MongoDbBackedContentStore;
import org.uriplay.persistence.content.mongo.MongoRoughSearch;
import org.uriplay.persistence.equiv.EquivalentContentMergingContentWriter;
import org.uriplay.persistence.tracking.ContentMentionStore;
import org.uriplay.persistence.tracking.MongoDBBackedContentMentionStore;

import com.mongodb.Mongo;

@Configuration
public class UriplayPersistenceModule {

	private @Autowired Mongo mongo;
	
	public @Bean ContentMentionStore contentMentionStore() {
		return new MongoDBBackedContentMentionStore(mongo, "uriplay");
	}
	
	public @Bean ContentWriter contentWriter() {
		return new EquivalentContentMergingContentWriter(new EventFiringContentWriter(mongoContentStore(), contentListener()));
	}	
	
	public @Bean MongoRoughSearch mongoRoughSearch() {
		return new MongoRoughSearch(mongoContentStore());
	}
	
	@Bean MongoDbBackedContentStore mongoContentStore() {
		return new MongoDbBackedContentStore(mongo, "uriplay");
	}
	
	@Bean MongoDbBackedContentBootstrapper contentBootstrapper() {
		return new MongoDbBackedContentBootstrapper(contentListener(), mongoContentStore());
	}
	
	
	public @Bean(destroyMethod="shutdown") ContentListener contentListener() {
		return new QueueingContentListener(aggregateListener());
	}

	@Bean AggregateContentListener aggregateListener() {
		return new AggregateContentListener();
	}
}
