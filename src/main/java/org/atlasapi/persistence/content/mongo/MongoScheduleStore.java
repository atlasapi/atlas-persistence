package org.atlasapi.persistence.content.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Channel;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.ContentGroup;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.media.entity.Version;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.media.entity.ScheduleEntryTranslator;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.time.DateTimeZones;
import com.mongodb.DBCollection;

public class MongoScheduleStore implements ContentWriter, ScheduleResolver {

    private final DBCollection collection;
    private final ScheduleEntryTranslator translator = new ScheduleEntryTranslator();
    private final long binMillis = 3600000;

    public MongoScheduleStore(DatabasedMongo db) {
        collection = db.collection("schedule");
    }

    @Override
    public void createOrUpdate(Item item) {
        createOrUpdate(ImmutableList.of(item));
    }

    @Override
    public void createOrUpdate(Container<? extends Item> container, boolean markMissingItemsAsUnavailable) {
        createOrUpdate(container.getContents());
    }

    private void createOrUpdate(Iterable<? extends Item> items) {
        for (ScheduleEntry entry: toScheduleEntries(items)) {
            List<ScheduleEntry> entries = translator.fromDbObjects(where().idEquals(entry.toKey()).find(collection));
            
            ScheduleEntry updateEntry;
            if (entries.isEmpty()) {
                updateEntry = entry;
            } else {
                updateEntry = Iterables.getOnlyElement(entries);
                updateEntry.withItems(mergeItems(updateEntry.items(), entry.items()));
            }
            
            collection.save(translator.toDb(updateEntry));
        }
    }
    
    private List<Item> mergeItems(Iterable<Item> original, Iterable<Item> latest) {
        return Ordering.from(ScheduleEntry.START_TIME_ITEM_COMPARATOR).immutableSortedCopy(Sets.union(ImmutableSet.copyOf(latest), ImmutableSet.copyOf(original)));
    }
    
    private List<ScheduleEntry> toScheduleEntries(Iterable<? extends Item> items) {
        Map<String, ScheduleEntry> entries = Maps.newHashMap();

        for (Item item : items) {
            for (Version version : item.nativeVersions()) {
                for (Broadcast broadcast : version.getBroadcasts()) {
                    Version entryVersion = version.copy();
                    entryVersion.setBroadcasts(ImmutableSet.of(broadcast.copy()));
                    Item entryItem = (Item) item.copy();
                    entryItem.setVersions(ImmutableSet.of(entryVersion));

                    Channel channel = Channel.fromUri(broadcast.getBroadcastOn()).requireValue();
                    Publisher publisher = item.getPublisher();
                    for (Interval interval: intervalsFor(broadcast.getTransmissionTime(), broadcast.getTransmissionEndTime())) {
                        String key = ScheduleEntry.toKey(interval, channel, publisher);

                        if (entries.containsKey(key)) {
                            ScheduleEntry entry = entries.get(key);
                            entry.withItems(ImmutableList.<Item> builder().addAll(entry.items()).add(entryItem).build());
                        } else {
                            ScheduleEntry entry = new ScheduleEntry(interval, channel, publisher, ImmutableList.of(entryItem));
                            entries.put(key, entry);
                        }
                    }
                }
            }
        }

        return Ordering.natural().immutableSortedCopy(entries.values());
    }

    @Override
    public Schedule schedule(DateTime from, DateTime to, Iterable<Channel> channels, Iterable<Publisher> publishers) {
        Map<Channel, List<Item>> channelMap = createChannelMap(channels);
        Interval interval = new Interval(from, to);
        List<ScheduleEntry> entries = translator.fromDbObjects(where().idIn(keys(intervalsFor(from, to), channels, publishers)).find(collection));
        
        for (ScheduleEntry entry: entries) {
            channelMap.get(entry.channel()).addAll(entry.items());
        }
        
        ImmutableMap.Builder<Channel, List<Item>> processedChannelMap = ImmutableMap.builder();
        for (Entry<Channel, List<Item>> entry: channelMap.entrySet()) {
            processedChannelMap.put(entry.getKey(), processChannelItems(entry.getValue(), interval));
        }
        return Schedule.fromChannelMap(processedChannelMap.build(), interval);
    }
    
    private Map<Channel, List<Item>> createChannelMap(Iterable<Channel> channels) {
        ImmutableMap.Builder<Channel, List<Item>> channelMap = ImmutableMap.builder();
        for (Channel channel : channels) {
            channelMap.put(channel, Lists.<Item> newArrayList());
        }
        return channelMap.build();
    }
    
    private List<Item> processChannelItems(Iterable<Item> items, final Interval interval) {
        return orderItems(filterItems(items, interval));
    }
    
    private List<Item> orderItems(Iterable<Item> items) {
        return Ordering.from(ScheduleEntry.START_TIME_ITEM_COMPARATOR).immutableSortedCopy(items);
    }
    
    private Iterable<Item> filterItems(Iterable<Item> items, final Interval interval) {
        final Predicate<Item> validBroadcast = new Predicate<Item>() {
            @Override
            public boolean apply(Item item) {
                Broadcast broadcast = ScheduleEntry.BROADCAST.apply(item);
                return interval.overlaps(new Interval(broadcast.getTransmissionTime(), broadcast.getTransmissionEndTime()));
            }
        };
        
        return Iterables.transform(ImmutableSet.copyOf(Iterables.transform(Iterables.filter(items, validBroadcast), ItemScheduleEntry.ITEM_SCHEDULE_ENTRY)), ItemScheduleEntry.ITEM);
    }
    
    private List<String> keys(Iterable<Interval> intervals, Iterable<Channel> channels, Iterable<Publisher> publishers) {
        ImmutableList.Builder<String> keys = ImmutableList.builder();
        for (Interval interval: intervals) {
            for (Channel channel: channels) {
                for (Publisher publisher: publishers) {
                    keys.add(ScheduleEntry.toKey(interval, channel, publisher));
                }
            }
        }
        return keys.build();
    }
    
    private long millisBackToNearestBin(DateTime when) {
        long div = when.getMillis() / binMillis;
        int asInt = (int) div;
        return asInt * binMillis;
    }
    
    private List<Interval> intervalsFor(DateTime start, DateTime end) {
        long startMillis = millisBackToNearestBin(start);
        long endMillis = end.getMillis();
        ImmutableList.Builder<Interval> intervals = ImmutableList.builder();

        while (startMillis < endMillis) {
            Interval interval = new Interval(new DateTime(startMillis, DateTimeZones.UTC), new DateTime(startMillis + binMillis - 1, DateTimeZones.UTC));
            intervals.add(interval);
            startMillis += (binMillis);
        }
        
        return intervals.build();
    }

    @Override
    public void createOrUpdateSkeleton(ContentGroup playlist) {
        throw new UnsupportedOperationException("Schedule Store is not interested in your pathetic groupings. Be gone.");
    }
    
    private static class ItemScheduleEntry {
        
        private final Item item;
        
        public ItemScheduleEntry(Item item) {
            Preconditions.checkNotNull(item);
            this.item = item;
        }
        
        public Item item() {
            return item;
        }
        
        public DateTime startTime() {
            return ScheduleEntry.BROADCAST.apply(item).getTransmissionTime();
        }
        
        public String canonicalUri() {
            return item.getCanonicalUri();
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ItemScheduleEntry) {
                ItemScheduleEntry entry = (ItemScheduleEntry) obj;
                return startTime().equals(entry.startTime()) && canonicalUri().equals(entry.canonicalUri());
            }
            return super.equals(obj);
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(startTime(), canonicalUri());
        }
        
        final static Function<ItemScheduleEntry, Item> ITEM = new Function<ItemScheduleEntry, Item>() {
            @Override
            public Item apply(ItemScheduleEntry input) {
                return input.item();
            }
        };
        
        final static Function<Item, ItemScheduleEntry> ITEM_SCHEDULE_ENTRY = new Function<Item, ItemScheduleEntry>() {
            @Override
            public ItemScheduleEntry apply(Item input) {
                return new ItemScheduleEntry(input);
            }
        };
    }
}
