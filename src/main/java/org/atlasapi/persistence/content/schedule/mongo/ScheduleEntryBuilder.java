package org.atlasapi.persistence.content.schedule.mongo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Channel;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.media.entity.Version;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.time.DateTimeZones;

public class ScheduleEntryBuilder {
    
    private final static long BIN_MILLIS = Duration.standardHours(1).getMillis();
    
    private final Duration maxBroadcastAge;

    public ScheduleEntryBuilder(Duration maxBroadcastAge) {
        this.maxBroadcastAge = maxBroadcastAge;
    }
    
    public ScheduleEntryBuilder() {
        this(Duration.standardDays(28));
    }
    
    public Map<String, ScheduleEntry> toScheduleEntries(Iterable<? extends Item> items) {
    	Map<String, ScheduleEntry> entries = new HashMap<String, ScheduleEntry>();
    	
        for (Item item : items) {
            for (Version version : item.nativeVersions()) {
                for (Broadcast broadcast : version.getBroadcasts()) {  
                    ItemRefAndBroadcast itemAndBroadcast = new ItemRefAndBroadcast(item, broadcast);
                    
                    Channel channel = Channel.fromUri(broadcast.getBroadcastOn()).requireValue();
                    Publisher publisher = item.getPublisher();
                    
                    toScheduleEntryFromBroadcast(channel, publisher, itemAndBroadcast, entries);
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
		
	    for (Interval interval: intervalsFor(broadcast.getTransmissionTime(), broadcast.getTransmissionEndTime())) {
	        String key = ScheduleEntry.toKey(interval, channel, publisher);

	        if (entries.containsKey(key)) {
	            ScheduleEntry entry = entries.get(key);
	            entry.withItems(Iterables.concat(entry.getItemRefsAndBroadcasts(), ImmutableList.of(itemAndBroadcast)));
	        } else {
	            ScheduleEntry entry = new ScheduleEntry(interval, channel, publisher, ImmutableList.of(itemAndBroadcast));
	            entries.put(key, entry);
	        }
	    }
	}
    
    List<Interval> intervalsFor(DateTime start, DateTime end) {
        if (start.isBefore(new DateTime(DateTimeZones.UTC).minus(maxBroadcastAge))) {
            return ImmutableList.of();
        }
        
        long startMillis = millisBackToNearestBin(start);
        long endMillis = end.getMillis();
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
