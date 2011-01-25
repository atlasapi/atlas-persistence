package org.atlasapi.persistence;

import org.atlasapi.persistence.content.AggregateContentListener;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.mongo.MongoDbBackedContentStore;

public interface ContentPersistenceModule {

	ContentWriter persistentWriter();
	
	MongoDbBackedContentStore contentStore();

	AggregateContentListener contentListener();
	
}
