package org.atlasapi.persistence;

import javax.annotation.PostConstruct;

import org.atlasapi.persistence.content.AggregateContentListener;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.EventFiringContentWriter;
import org.atlasapi.persistence.content.mongo.FullMongoScheduleRepopulator;
import org.atlasapi.persistence.content.mongo.MongoDbBackedContentStore;
import org.atlasapi.persistence.content.mongo.MongoScheduleStore;
import org.atlasapi.persistence.content.mongo.ScheduleUpdatingContentListener;
import org.atlasapi.persistence.shorturls.MongoShortUrlSaver;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import com.metabroadcast.common.time.DayOfWeek;

@Configuration
public class MongoContentPersistenceModule implements ContentPersistenceModule {

	private @Autowired DatabasedMongo db;
	private @Autowired SimpleScheduler scheduler;
	
	@PostConstruct
	public void installScheduleRebuilder() {
		scheduler.schedule(scheduleRepopulator(), RepetitionRules.weekly(DayOfWeek.SUNDAY, new LocalTime(1, 0, 0)));
	}
	
	public @Bean ContentWriter persistentWriter() {
		return eventFiringContentWriter();
	}
	
	public @Bean(name={"mongoContentStore"}) MongoDbBackedContentStore contentStore() {
		return new MongoDbBackedContentStore(db);
	}
	
	public @Bean MongoScheduleStore scheduleStore() {
	    return new MongoScheduleStore(db);
	}
	
	public @Bean FullMongoScheduleRepopulator scheduleRepopulator() {
	    return new FullMongoScheduleRepopulator(db, scheduleStore());
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
