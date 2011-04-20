package org.atlasapi.persistence;

import org.atlasapi.persistence.content.AggregateContentListener;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.QueuingPersonWriter;
import org.atlasapi.persistence.content.mongo.MongoDbBackedContentStore;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;

public interface ContentPersistenceModule {

	ContentWriter persistentWriter();
	
	QueuingPersonWriter queuingPersonWriter();
	
	MongoDbBackedContentStore contentStore();

	AggregateContentListener contentListener();

	ShortUrlSaver shortUrlSaver();
	
}
