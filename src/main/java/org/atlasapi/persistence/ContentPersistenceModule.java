package org.atlasapi.persistence;

import org.atlasapi.persistence.content.AggregateContentListener;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.mongo.MongoDbBackedContentStore;
import org.atlasapi.persistence.content.mongo.MongoRoughSearch;
import org.atlasapi.persistence.tracking.ContentMentionStore;

public interface ContentPersistenceModule {

	ContentMentionStore contentMentionStore();
	
	ContentWriter persistentWriter();
	
	MongoRoughSearch roughSearch();
	
	MongoDbBackedContentStore contentStore();

	AggregateContentListener contentListener();
	
}
