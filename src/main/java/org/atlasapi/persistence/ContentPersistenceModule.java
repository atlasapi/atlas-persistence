package org.atlasapi.persistence;

import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;
import org.atlasapi.persistence.topic.TopicStore;

public interface ContentPersistenceModule {

	ContentWriter contentWriter();
	
	ItemsPeopleWriter itemsPeopleWriter();
	
	ContentResolver contentResolver();
	
	TopicStore topicStore();

	ShortUrlSaver shortUrlSaver();
	
}
