package org.atlasapi.persistence;

import javax.annotation.PostConstruct;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.mongo.FullMongoScheduleRepopulator;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.joda.time.Duration;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
public class ManualScheduleRebuildModule {

	private @Autowired DatabasedMongo db;
	private @Autowired SimpleScheduler scheduler;
	private @Autowired MongoScheduleStore scheduleStore;
	
	@PostConstruct
	public void installScheduleRebuilder() {
	    ScheduledTask everythingRepopulator =
	    	new FullMongoScheduleRepopulator(db, scheduleStore, ImmutableList.<Publisher>of())
	    	.withName("Full Mongo Schedule repopulator");
	    
	    scheduler.schedule(everythingRepopulator, RepetitionRules.daily(new LocalTime(1, 15, 0)));
		
	    ScheduledTask bbcRepopulator = 
	    	new FullMongoScheduleRepopulator(db, scheduleStore, ImmutableList.<Publisher>of(Publisher.BBC))
	    	.withName("BBC Mongo Schedule repopulator");
	    
        scheduler.schedule(bbcRepopulator, RepetitionRules.every(Duration.standardHours(2)));
        
        ScheduledTask c4Repopulator = 
        	new FullMongoScheduleRepopulator(db, scheduleStore, ImmutableList.<Publisher>of(Publisher.C4))
        	.withName("C4 Mongo Schedule repopulator");
        
        scheduler.schedule(c4Repopulator, RepetitionRules.every(Duration.standardHours(1)).withOffset(Duration.standardMinutes(30)));
	}
}
