package org.atlasapi.persistence;

import javax.annotation.PostConstruct;

import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.mongo.FullMongoScheduleRepopulator;
import org.atlasapi.persistence.content.schedule.mongo.MongoScheduleStore;
import org.joda.time.Duration;
import org.joda.time.LocalTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
public class ManualScheduleRebuildModule {

    private @Autowired SimpleScheduler scheduler;
	
    private @Autowired MongoScheduleStore scheduleStore;
	private @Autowired ContentLister lister;
	private @Autowired ChannelResolver channelResolver;
	
	@PostConstruct
	public void installScheduleRebuilder() {
	    ScheduledTask everythingRepopulator =
	    	new FullMongoScheduleRepopulator(lister, channelResolver, scheduleStore, ImmutableList.<Publisher>of())
	    	.withName("Full Mongo Schedule repopulator");
	    
	    scheduler.schedule(everythingRepopulator, RepetitionRules.daily(new LocalTime(3, 15, 0)));
		
	    ScheduledTask bbcRepopulator = 
	    	new FullMongoScheduleRepopulator(lister, channelResolver, scheduleStore, ImmutableList.<Publisher>of(Publisher.BBC))
	    	.withName("BBC Mongo Schedule repopulator");
	    
        scheduler.schedule(bbcRepopulator, RepetitionRules.every(Duration.standardHours(2)));
        
        ScheduledTask c4Repopulator = 
        	new FullMongoScheduleRepopulator(lister, channelResolver, scheduleStore, ImmutableList.<Publisher>of(Publisher.C4))
        	.withName("C4 Mongo Schedule repopulator");
        
        scheduler.schedule(c4Repopulator, RepetitionRules.every(Duration.standardHours(1)).withOffset(Duration.standardMinutes(30)));
        
        ScheduledTask reduxRepopulator = 
                new FullMongoScheduleRepopulator(lister, channelResolver, scheduleStore, ImmutableList.<Publisher>of(Publisher.BBC_REDUX), Duration.standardDays(30*365))
        .withName("Redux Mongo Schedule repopulator");
    
        scheduler.schedule(reduxRepopulator, RepetitionRules.NEVER);
	}
}
