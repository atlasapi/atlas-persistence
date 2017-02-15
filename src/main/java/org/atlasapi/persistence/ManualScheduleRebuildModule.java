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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.scheduling.RepetitionRules;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.metabroadcast.common.scheduling.SimpleScheduler;

@Configuration
/**
 * Some adapters do not maintain schedules for their content. These adapters have scheduled
 * tasks to build the schedule from broadcasts on a periodic basis.
 *
 * The tasks that are scheduled to never run can be used to correct a schedule if it becomes
 * out of synch with the contents' broadcasts.
 */
public class ManualScheduleRebuildModule {

    private @Autowired SimpleScheduler scheduler;
	
    private @Autowired MongoScheduleStore scheduleStore;
	private @Autowired ContentLister lister;
	private @Autowired ChannelResolver channelResolver;
	
	private @Value("${schedule.repopulator.full.scheduled}") boolean fullScheduleRepopulatorScheduled;
	private @Value("${schedule.repopulator.bbc.scheduled}") boolean bbcScheduleRepopulatorScheduled;
	private @Value("${schedule.repopulator.redux.scheduled}") boolean reduxScheduleRepopulatorScheduled;
	
	@PostConstruct
	public void installScheduleRebuilder() {
	    ScheduledTask everythingRepopulator =
	    	new FullMongoScheduleRepopulator(lister, channelResolver, scheduleStore, ImmutableList.<Publisher>of())
	    	.withName("Full Mongo Schedule repopulator");
	    
	    scheduler.schedule(everythingRepopulator, fullScheduleRepopulatorScheduled ? RepetitionRules.daily(new LocalTime(3, 15, 0)) : RepetitionRules.NEVER);

        ScheduledTask c4PmlsdRepopulator = 
                new FullMongoScheduleRepopulator(lister, channelResolver, scheduleStore, ImmutableList.<Publisher>of(Publisher.C4_PMLSD, Publisher.C4_PMLSD_P06))
                .withName("C4 PMLSD Mongo Schedule repopulator");
        
        // This is a one-off schedule population, future schedule populations will done
        // through the adapter
        scheduler.schedule(c4PmlsdRepopulator, RepetitionRules.NEVER);
        
        ScheduledTask reduxRepopulator = 
                new FullMongoScheduleRepopulator(lister, channelResolver, scheduleStore, ImmutableList.<Publisher>of(Publisher.BBC_REDUX), Duration.standardDays(30L*365))
        .withName("Redux Mongo Schedule repopulator");
    
        scheduler.schedule(reduxRepopulator, reduxScheduleRepopulatorScheduled ? RepetitionRules.every(Duration.standardHours(1)) : RepetitionRules.NEVER);
        
        ScheduledTask youViewRepopulator = 
                new FullMongoScheduleRepopulator(lister, channelResolver, scheduleStore, ImmutableList.<Publisher>of(Publisher.YOUVIEW), Duration.standardDays(30))
        .withName("YouView Mongo Schedule repopulator");
        
        scheduler.schedule(youViewRepopulator, RepetitionRules.NEVER);
	}
}
