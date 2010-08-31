package org.atlasapi.persistence;

import org.atlasapi.persistence.content.AggregateContentListener;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.DefinitiveContentWriter;
import org.atlasapi.persistence.content.mongo.MongoDbBackedContentStore;
import org.atlasapi.persistence.content.mongo.MongoRoughSearch;
import org.atlasapi.persistence.tracking.ContentMentionStore;

public interface ContentPersistenceModule {

	public ContentMentionStore contentMentionStore();
	
	public ContentWriter persistentWriter();
	
	public DefinitiveContentWriter definitiveWriter();
	
	public MongoRoughSearch roughSearch();
	
	public MongoDbBackedContentStore contentStore();

	public  AggregateContentListener contentListener();
	
}
