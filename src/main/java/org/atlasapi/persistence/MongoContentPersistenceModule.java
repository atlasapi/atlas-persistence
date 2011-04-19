package org.atlasapi.persistence;

import javax.annotation.PostConstruct;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.AggregateContentListener;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.EventFiringContentWriter;
import org.atlasapi.persistence.content.mongo.AsyncronousContentListener;
import org.atlasapi.persistence.content.mongo.FullMongoScheduleRepopulator;
import org.atlasapi.persistence.content.mongo.MongoDbBackedContentStore;
import org.atlasapi.persistence.content.mongo.MongoScheduleStore;
import org.atlasapi.persistence.content.mongo.ScheduleUpdatingContentListener;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.shorturls.MongoShortUrlSaver;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.SimpleScheduler;
import com.metabroadcast.common.time.DayOfWeek;
import com.mongodb.Mongo;

@Configuration
public class MongoContentPersistenceModule implements ContentPersistenceModule {

	private @Autowired DatabasedMongo db;
	private @Autowired SimpleScheduler scheduler;
	private @Value("${schedule.mongo.host}") String scheduleMongoHost;
	private @Value("${schedule.mongo.port}") String scheduleMongoPort;
	private @Value("${schedule.mongo.name}") String scheduleMongoName;
	private @Autowired AdapterLog log;
	
	@PostConstruct
	public void installScheduleRebuilder() {
	    FullMongoScheduleRepopulator everythingRepopulator = new FullMongoScheduleRepopulator(db, scheduleStore(), ImmutableList.<Publisher>of());
		scheduler.schedule(everythingRepopulator, RepetitionRules.weekly(DayOfWeek.SUNDAY, new LocalTime(1, 0, 0)));
		FullMongoScheduleRepopulator bbcRepopulator = new FullMongoScheduleRepopulator(db, scheduleStore(), ImmutableList.<Publisher>of(Publisher.BBC));
        scheduler.schedule(bbcRepopulator, RepetitionRules.daily(new LocalTime(21, 0, 0)));
        FullMongoScheduleRepopulator c4Repopulator = new FullMongoScheduleRepopulator(db, scheduleStore(), ImmutableList.<Publisher>of(Publisher.C4));
        scheduler.schedule(c4Repopulator, RepetitionRules.daily(new LocalTime(23, 0, 0)));
	}
	
	public @Bean ContentWriter persistentWriter() {
		return eventFiringContentWriter();
	}
	
	public @Bean(name={"mongoContentStore"}) MongoDbBackedContentStore contentStore() {
		return new MongoDbBackedContentStore(db);
	}
	
	public @Bean MongoScheduleStore scheduleStore() {
	    try {
            DatabasedMongo scheduleDb = ! Strings.isNullOrEmpty(scheduleMongoHost) && ! Strings.isNullOrEmpty(scheduleMongoName) && ! Strings.isNullOrEmpty(scheduleMongoPort)
                    ? new DatabasedMongo(new Mongo(scheduleMongoHost, Integer.parseInt(scheduleMongoPort)), scheduleMongoName)
                    : db;
            return new MongoScheduleStore(scheduleDb);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
	}
	
	@Bean EventFiringContentWriter eventFiringContentWriter() {
	    return new EventFiringContentWriter(contentStore(), contentListener());
	}

	public @Bean AggregateContentListener contentListener() {
	    AggregateContentListener listener = new AggregateContentListener();
	    listener.addListener(new AsyncronousContentListener(new ScheduleUpdatingContentListener(scheduleStore()), log));
	    return listener;
	}

	@Override
	public @Bean ShortUrlSaver shortUrlSaver() {
		return new MongoShortUrlSaver(db);
	}
}
