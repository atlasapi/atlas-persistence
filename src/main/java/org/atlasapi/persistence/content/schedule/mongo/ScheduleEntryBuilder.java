package org.atlasapi.persistence.content.schedule.mongo;

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
import com.google.common.collect.Maps;
import com.metabroadcast.common.time.DateTimeZones;

public class ScheduleEntryBuilder {
    
    private final static long BIN_MILLIS = Duration.standardHours(1).getMillis();
    private static final Duration MAX_BROADCAST_AGE = Duration.standardDays(28);
    
    public Map<String, ScheduleEntry> toScheduleEntries(Iterable<? extends Item> items) {
        Map<String, ScheduleEntry> entries = Maps.newHashMap();

        for (Item item : items) {
            for (Version version : item.nativeVersions()) {
                for (Broadcast broadcast : version.getBroadcasts()) {
                    if(!broadcast.isActivelyPublished() && Publisher.PA.equals(version.getProvider())) {
                        continue;
                    }
                    
                    ItemRefAndBroadcast itemAndBroadcast = new ItemRefAndBroadcast(item, broadcast);
                    
                    Channel channel = Channel.fromUri(broadcast.getBroadcastOn()).requireValue();
                    Publisher publisher = item.getPublisher();
                    
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
            }
        }

        return entries;
    }
    
    List<Interval> intervalsFor(DateTime start, DateTime end) {
        if (start.isBefore(new DateTime(DateTimeZones.UTC).minus(MAX_BROADCAST_AGE))) {
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
