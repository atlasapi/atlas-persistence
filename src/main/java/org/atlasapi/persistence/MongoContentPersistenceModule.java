package org.atlasapi.persistence;

import org.atlasapi.persistence.content.AggregateContentListener;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.EventFiringContentWriter;
import org.atlasapi.persistence.content.mongo.MongoDbBackedContentStore;
import org.atlasapi.persistence.content.mongo.MongoScheduleStore;
import org.atlasapi.persistence.content.mongo.ScheduleUpdatingContentListener;
import org.atlasapi.persistence.shorturls.MongoShortUrlSaver;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;

@Configuration
public class MongoContentPersistenceModule implements ContentPersistenceModule {

	private @Autowired DatabasedMongo db;
	
	
	public @Bean ContentWriter persistentWriter() {
		return eventFiringContentWriter();
	}
	
	public @Bean(name={"mongoContentStore"}) MongoDbBackedContentStore contentStore() {
		return new MongoDbBackedContentStore(db);
	}
	
	public @Bean MongoScheduleStore scheduleStore() {
	    return new MongoScheduleStore(db);
	}
	
	@Bean EventFiringContentWriter eventFiringContentWriter() {
	    return new EventFiringContentWriter(contentStore(), contentListener());
	}

	public @Bean AggregateContentListener contentListener() {
	    AggregateContentListener listener = new AggregateContentListener();
	    listener.addListener(new ScheduleUpdatingContentListener(scheduleStore()));
	    return listener;
	}

	@Override
	public @Bean ShortUrlSaver shortUrlSaver() {
		return new MongoShortUrlSaver(db);
	}
}
