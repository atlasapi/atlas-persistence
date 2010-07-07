package org.uriplay.persistence;

import org.uriplay.persistence.content.AggregateContentListener;
import org.uriplay.persistence.content.ContentWriter;
import org.uriplay.persistence.content.mongo.MongoDbBackedContentStore;
import org.uriplay.persistence.content.mongo.MongoRoughSearch;
import org.uriplay.persistence.tracking.ContentMentionStore;

public interface ContentPersistenceModule {

	public ContentMentionStore contentMentionStore();
	
	public ContentWriter persistentWriter();
	
	public MongoRoughSearch roughSearch();
	
	public MongoDbBackedContentStore contentStore();

	public  AggregateContentListener contentListener();
	
}
