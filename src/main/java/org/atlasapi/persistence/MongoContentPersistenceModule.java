package org.atlasapi.persistence;

import org.atlasapi.persistence.content.AggregateContentListener;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.EventFiringContentWriter;
import org.atlasapi.persistence.content.mongo.AsyncronousContentListener;
import org.atlasapi.persistence.content.mongo.MongoDbBackedContentStore;
import org.atlasapi.persistence.content.mongo.MongoPersonStore;
import org.atlasapi.persistence.content.people.ItemsPeopleWriter;
import org.atlasapi.persistence.content.people.QueuingItemsPeopleWriter;
import org.atlasapi.persistence.content.people.QueuingPersonWriter;
import org.atlasapi.persistence.content.schedule.mongo.BatchingScheduleWriter;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleUpdatingContentListener;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.shorturls.MongoShortUrlSaver;
import org.atlasapi.persistence.shorturls.ShortUrlSaver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.base.Strings;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.Mongo;

@Configuration
public class MongoContentPersistenceModule implements ContentPersistenceModule {

	private @Autowired DatabasedMongo db;

	private @Value("${schedule.mongo.host}") String scheduleMongoHost;
	private @Value("${schedule.mongo.port}") String scheduleMongoPort;
	private @Value("${schedule.mongo.name}") String scheduleMongoName;
	private @Autowired AdapterLog log;
	
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
	
	public @Bean ItemsPeopleWriter itemsPeopleWriter() {
	    return new QueuingItemsPeopleWriter(new QueuingPersonWriter(personStore(), log), log);
	}
	
	public @Bean MongoPersonStore personStore() {
	    return new MongoPersonStore(db);
	}
	
	public @Bean ScheduleWriter queueingScheduleWriter() {
		return new BatchingScheduleWriter(scheduleStore(), log);
	}
	

	@Bean EventFiringContentWriter eventFiringContentWriter() {
	    return new EventFiringContentWriter(contentStore(), contentListener());
	}

	public @Bean AggregateContentListener contentListener() {
	    AggregateContentListener listener = new AggregateContentListener();
	    listener.addListener(new AsyncronousContentListener(new ScheduleUpdatingContentListener(queueingScheduleWriter()), log));
	    return listener;
	}

	@Override
	public @Bean ShortUrlSaver shortUrlSaver() {
		return new MongoShortUrlSaver(db);
	}
}
