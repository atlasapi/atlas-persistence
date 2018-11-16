package org.atlasapi.persistence.content.schedule.mongo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.media.entity.Version;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.format.PeriodFormat;

public class ScheduleEntryBuilder {
    
    private final Log log = LogFactory.getLog(ScheduleEntryBuilder.class);
    
    private static final long BIN_MILLIS = Duration.standardHours(1).getMillis();
    private static final long MAX_ALLOWED_TOTAL_INTERVAL = Duration.standardDays(370).getMillis();
    
    private final Duration maxBroadcastAge;

	private ChannelResolver channelResolver;

    public ScheduleEntryBuilder(ChannelResolver channelResolver, Duration maxBroadcastAge) {
        this.maxBroadcastAge = maxBroadcastAge;
        this.channelResolver = channelResolver;
    }
    
    public ScheduleEntryBuilder(ChannelResolver channelResolver) {
        this(channelResolver, Duration.standardDays(28));
    }
    
    public Map<String, ScheduleEntry> toScheduleEntries(Iterable<? extends Item> items) {
    	Map<String, ScheduleEntry> entries = new HashMap<String, ScheduleEntry>();
    	
        for (Item item : items) {
            for (Version version : item.nativeVersions()) {
                for (Broadcast broadcast : version.getBroadcasts()) {  
                    ItemRefAndBroadcast itemAndBroadcast = new ItemRefAndBroadcast(item, broadcast);
                    Publisher publisher = item.getPublisher();
                    
                    Maybe<Channel> channel = channelResolver.fromUri(broadcast.getBroadcastOn());
                    if (channel.hasValue()) {
                        toScheduleEntryFromBroadcast(channel.requireValue(), publisher, itemAndBroadcast, entries);
                    } else {
                        log.warn("No channel for " + broadcast.getTransmissionTime().toString() + " of " + item.getCanonicalUri());
                    }
                }
            }
        }

        return entries;
    }

	public void toScheduleEntryFromBroadcast(Channel channel, Publisher publisher, ItemRefAndBroadcast itemAndBroadcast, Map<String, ScheduleEntry> entries) {
	    
		Broadcast broadcast = itemAndBroadcast.getBroadcast();
		if(!broadcast.isActivelyPublished() && Publisher.PA.equals(publisher)) {
			return;
		}
		
		if(!broadcast.getTransmissionTime().isBefore(new DateTime(DateTimeZones.UTC).minus(maxBroadcastAge))) {
		    for (Interval interval: intervalsFor(broadcast.getTransmissionTime(), broadcast.getTransmissionEndTime())) {
		        String key = ScheduleEntry.toKey(interval, channel, publisher);
	
		        if (entries.containsKey(key)) {
		            ScheduleEntry entry = entries.get(key);
		            // I think here we should not keep the current entry broadcasts, but only the new ones
//		            entry.withItems(Iterables.concat(entry.getItemRefsAndBroadcasts(), ImmutableList.of(itemAndBroadcast)));
                    entry.withItems(ImmutableList.copyOf(ImmutableList.of(itemAndBroadcast)));
		        } else {
		            ScheduleEntry entry = new ScheduleEntry(interval, channel, publisher, ImmutableList.of(itemAndBroadcast));
		            entries.put(key, entry);
		        }
		    }
		}
	}
    
    List<Interval> intervalsFor(DateTime start, DateTime end) {
        
        long startMillis = millisBackToNearestBin(start);
        long endMillis = end.getMillis();

        //it is not the place of this function to impose restrictions, but if the difference is
        //huge it is highly probable that something went wrong, and the loop below will
        //create an out of memory situation anyway. Prevent that by throwing an exception that has
        //higher chances of being handled.
        if (endMillis - startMillis > MAX_ALLOWED_TOTAL_INTERVAL) {
            log.error("Cannot create intervals for a total duration greater than " +
                      PeriodFormat.getDefault().print(Duration.millis(MAX_ALLOWED_TOTAL_INTERVAL).toPeriod()));
            throw new IllegalArgumentException(
                    "Cannot create intervals for a total duration greater than "
                    + PeriodFormat.getDefault().print(Duration.millis(MAX_ALLOWED_TOTAL_INTERVAL).toPeriod())
                    + ". This is to protect us from out of memory errors.");
        }

        ImmutableList.Builder<Interval> intervals = ImmutableList.builder();
        while (startMillis < endMillis) {
            Interval interval = new Interval(new DateTime(startMillis, DateTimeZones.UTC), new DateTime(startMillis + BIN_MILLIS - 1, DateTimeZones.UTC));
            intervals.add(interval);
            startMillis += (BIN_MILLIS);
        }
        
        return intervals.build();
    }
    
    private long millisBackToNearestBin(DateTime when) {
        long div = when.getMillis() / BIN_MILLIS;
        int asInt = (int) div;
        return asInt * BIN_MILLIS;
    }
}
