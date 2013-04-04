package org.atlasapi.persistence.content.schedule.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.atlasapi.application.ApplicationConfiguration;
import org.atlasapi.equiv.OutputContentMerger;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Broadcast;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Encoding;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Location;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.media.entity.ScheduleEntry.ItemRefAndBroadcast;
import org.atlasapi.media.entity.Version;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.EquivalentContent;
import org.atlasapi.persistence.content.EquivalentContentResolver;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.schedule.ScheduleBroadcastFilter;
import org.atlasapi.persistence.media.channel.ChannelResolver;
import org.atlasapi.persistence.media.entity.ScheduleEntryTranslator;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import com.fasterxml.jackson.databind.util.Annotations;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.base.MorePredicates;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.mongodb.DBCollection;

public class MongoScheduleStore implements ScheduleResolver, ScheduleWriter {

	private final static Duration MAX_DURATION = Duration.standardDays(14);

	private final ScheduleEntryBuilder scheduleEntryBuilder;
    private final DBCollection collection;
    private final ScheduleEntryTranslator translator;

    private final ContentResolver resolver;
    private final EquivalentContentResolver equivalentContentResolver;
    private final ChannelResolver channelResolver;


    public MongoScheduleStore(DatabasedMongo db, ContentResolver resolver, ChannelResolver channelResolver, EquivalentContentResolver equivalentContentResolver) {
        this.resolver = resolver;
        this.equivalentContentResolver = equivalentContentResolver;
        collection = db.collection("schedule");
        this.channelResolver = channelResolver;
        this.scheduleEntryBuilder = new ScheduleEntryBuilder(channelResolver, Duration.standardDays(28));
        translator = new ScheduleEntryTranslator(channelResolver);
    }
    
    @Override
    public void writeCompleteEntry(ScheduleEntry entry) {
        collection.save(translator.toDb(entry));
    }

    @Override
    public void writeScheduleFor(Iterable<? extends Item> items) {
        Map<String, ScheduleEntry> scheduleEntries = scheduleEntryBuilder.toScheduleEntries(items);
        Map<String, ScheduleEntry> existingEntries = Maps.uniqueIndex(translator.fromDbObjects(where().idIn(scheduleEntries.keySet()).find(collection)), ScheduleEntry.KEY);
        
        for (ScheduleEntry entry: scheduleEntries.values()) {
            ScheduleEntry updateEntry;
            ScheduleEntry existingEntry = existingEntries.get(entry.toKey());
            if (existingEntry == null) {
                updateEntry = entry;
            } else {
				updateEntry = existingEntry;
                updateEntry.withItems(Iterables.concat(updateEntry.getItemRefsAndBroadcasts(), entry.getItemRefsAndBroadcasts()));
            }
            writeCompleteEntry(updateEntry);
        }
    }

    @Override
    public void replaceScheduleBlock(Publisher publisher, Channel channel, Iterable<ItemRefAndBroadcast> itemsAndBroadcasts) {
    	
    	Interval interval = checkAndGetScheduleInterval(itemsAndBroadcasts, true, channel);
    	Map<String, ScheduleEntry> entries = getAdjacentScheduleEntries(channel, publisher, interval);
    	
    	for(ItemRefAndBroadcast itemAndBroadcast : itemsAndBroadcasts) {
	    	scheduleEntryBuilder.toScheduleEntryFromBroadcast(channel, publisher, itemAndBroadcast, entries);
    	}
    	for(ScheduleEntry entry : entries.values()) {
    		writeCompleteEntry(entry);
    	}
    }
    
    private Interval checkAndGetScheduleInterval(Iterable<ItemRefAndBroadcast> itemsAndBroadcasts, boolean allowGaps, Channel expectedChannel)
    {
		List<Broadcast> broadcasts = Lists.newArrayList();
		for(ItemRefAndBroadcast itemAndBroadcast : itemsAndBroadcasts) {
			broadcasts.add(itemAndBroadcast.getBroadcast());
		}
		
		Collections.<Broadcast>sort(broadcasts, new Comparator<Broadcast>() {
	
			@Override
			public int compare(Broadcast o1, Broadcast o2) {
				if(o1.getTransmissionTime().equals(o2.getTransmissionTime())) {
					return o1.getTransmissionEndTime().isBefore(o2.getTransmissionEndTime()) ? -1 : 1;
				}
				else {
					return o1.getTransmissionTime().isBefore(o2.getTransmissionTime()) ? -1 : 1;
				}
			}
		});
	
		DateTime currentEndTime = null;
		
		for(Broadcast b : broadcasts) {
			
			if(!expectedChannel.equals(channelResolver.fromUri(b.getBroadcastOn()).requireValue())) {
				throw new IllegalArgumentException("All broadcasts must be on the same channel");
			}	
			
			if(allowGaps) {
				if(currentEndTime != null && b.getTransmissionTime().isBefore(currentEndTime)) {
					throw new IllegalArgumentException("Overlapping periods found in schedule");
				}
			}
			else {
				if(currentEndTime != null && !(currentEndTime.equals(b.getTransmissionTime()))) {
					throw new IllegalArgumentException("Schedule is not contiguous");
				}
			}
			currentEndTime = b.getTransmissionEndTime();
		}
		
		return new Interval(broadcasts.get(0).getTransmissionTime(), currentEndTime);
    }
   
	private Map<String, ScheduleEntry> getAdjacentScheduleEntries(Channel channel,
			Publisher publisher,
			final Interval interval) {
		
        List<Interval> intervals = scheduleEntryBuilder.intervalsFor(interval.getStart(), interval.getEnd()); 
        Set<String> requiredScheduleEntries = Sets.<String>newHashSet();

	    String firstScheduleEntryKey = ScheduleEntry.toKey(intervals.get(0), channel, publisher);
	    String lastScheduleEntryKey = ScheduleEntry.toKey(intervals.get(intervals.size() - 1), channel, publisher);
	        
	    requiredScheduleEntries.add(firstScheduleEntryKey);
	    requiredScheduleEntries.add(lastScheduleEntryKey);
	    Map<String, ScheduleEntry> scheduleEntries = Maps.newHashMap(Maps.uniqueIndex(translator.fromDbObjects(where().idIn(requiredScheduleEntries).find(collection)), ScheduleEntry.KEY));
	    
	    // For the first period, retain items that start before the interval 
	    if(scheduleEntries.containsKey(firstScheduleEntryKey)) {
		    ScheduleEntry firstEntry = filteredScheduleEntry(scheduleEntries.get(firstScheduleEntryKey), new Predicate<ItemRefAndBroadcast>() {
				@Override
				public boolean apply(ItemRefAndBroadcast i) {	
					return (i.getBroadcast().getTransmissionTime().isBefore(interval.getStart()));
				}
			});
		    scheduleEntries.put(firstScheduleEntryKey, firstEntry);
	    }
	  
	    // For the last period, retain items that start on or after the end of the interval
	    if(scheduleEntries.containsKey(lastScheduleEntryKey)) {
		    ScheduleEntry lastEntry = filteredScheduleEntry(scheduleEntries.get(lastScheduleEntryKey), new Predicate<ItemRefAndBroadcast>() {
				@Override
				public boolean apply(ItemRefAndBroadcast i) {			
					return ! i.getBroadcast().getTransmissionTime().isBefore(interval.getEnd());
				}
			});
		    scheduleEntries.put(lastScheduleEntryKey, lastEntry);
	    }
	    return scheduleEntries;
	}
    
	private ScheduleEntry filteredScheduleEntry(ScheduleEntry entry, Predicate<ItemRefAndBroadcast> filterPredicate)  {
	
		Iterable<ItemRefAndBroadcast> filteredBroadcasts = Collections2.filter(entry.getItemRefsAndBroadcasts(), filterPredicate);
		ScheduleEntry filteredEntry = new ScheduleEntry(entry.interval(), entry.channel(), entry.publisher(), filteredBroadcasts);
		return filteredEntry;
    }
		
    @Override
    public Schedule schedule(DateTime from, DateTime to, Iterable<Channel> channels, Iterable<Publisher> publishers, Optional<ApplicationConfiguration> mergeConfig) {
        Map<Channel, List<Item>> channelMap = createChannelMap(channels);
        Interval interval = new Interval(from, to);
        if (interval.toDuration().isLongerThan(MAX_DURATION)) {
            throw new IllegalArgumentException("You cannot request more than 2 weeks of schedule");
        }
        List<ScheduleEntry> entries = translator.fromDbObjects(where().idIn(keys(scheduleEntryBuilder.intervalsFor(from, to), channels, publishers)).find(collection));

        
        for (ScheduleEntry entry: entries) {
            // TODO this code inefficient, but in future we should avoid hydrating then items
            // unless explicitly requested
            for (ItemRefAndBroadcast itemRefAndBroadcast : entry.getItemRefsAndBroadcasts()) {
                Maybe<Identified> possibleItem;
                if (mergeConfig.isPresent() && mergeConfig.get().precedenceEnabled()) {
                    possibleItem = findAndMerge(mergeConfig.get(), itemRefAndBroadcast);
                } else {
                    possibleItem = resolver.findByCanonicalUris(ImmutableList.of(itemRefAndBroadcast.getItemUri())).getFirstValue();
                }
                if (possibleItem.hasValue()) {
                    Item item = ((Item) possibleItem.requireValue()).copy();
                    if (selectAndTrimBroadcast(item, itemRefAndBroadcast.getBroadcast())) {
                        channelMap.get(entry.channel()).add(item);
                    }
                }
            }
        }
        
        ImmutableMap.Builder<Channel, List<Item>> processedChannelMap = ImmutableMap.builder();
        for (Entry<Channel, List<Item>> entry: channelMap.entrySet()) {
            processedChannelMap.put(entry.getKey(), processChannelItems(entry.getValue(), interval));
        }
        return Schedule.fromChannelMap(processedChannelMap.build(), interval);
    }

    private Maybe<Identified> findAndMerge(ApplicationConfiguration mergeConfig,
                                           ItemRefAndBroadcast itemRefAndBroadcast) {
        String uri = itemRefAndBroadcast.getItemUri();
        EquivalentContent resolved = equivalentContentResolver.resolveUris(ImmutableList.of(uri), mergeConfig.getEnabledSources(), Annotation.defaultAnnotations(), false);
        Set<Content> equivalents = resolved.get(uri);
        if (equivalents == null || equivalents.isEmpty()) {
            return Maybe.nothing();
        }
        return Maybe.<Identified>just(merger.merge(mergeConfig, ImmutableList.copyOf(equivalents)).get(0));
    }

    private boolean selectAndTrimBroadcast(Item item, Broadcast scheduleBroadcast) {
        boolean found = false;
        for (Version version : item.getVersions()) {
            Set<Broadcast> allBroadcasts = version.getBroadcasts();
            version.setBroadcasts(Sets.<Broadcast>newHashSet());
            if (found) {
                continue;
            }
            for (Broadcast broadcast : allBroadcasts) {
                if (scheduleBroadcast.equals(broadcast) && broadcast.isActivelyPublished()) {
                    version.setBroadcasts(Sets.newHashSet(broadcast));
                    found = true;
                }
            }
        }
        return found;
    }
    
    private Map<Channel, List<Item>> createChannelMap(Iterable<Channel> channels) {
        ImmutableMap.Builder<Channel, List<Item>> channelMap = ImmutableMap.builder();
        for (Channel channel : channels) {
            channelMap.put(channel, Lists.<Item> newArrayList());
        }
        return channelMap.build();
    }
    
    private List<Item> processChannelItems(Iterable<Item> items, final Interval interval) {
        return filterLocations(orderItems(filterItems(items, interval)));
    }
    
    private List<Item> orderItems(Iterable<Item> items) {
        return Ordering.from(ScheduleEntry.START_TIME_ITEM_COMPARATOR).immutableSortedCopy(items);
    }
    
    private static final Function<Broadcast, Interval> TO_BROADCAST = new Function<Broadcast, Interval>() {
        @Override
        public Interval apply(Broadcast input) {
            return new Interval(input.getTransmissionTime(), input.getTransmissionEndTime());
        }
    };

    private static final Function<Item, Interval> ITEM_TO_BROADCAST_INTERVAL = Functions.compose(TO_BROADCAST, ScheduleEntry.BROADCAST);

    private final OutputContentMerger merger = new OutputContentMerger();
    
    private Iterable<Item> filterItems(Iterable<Item> items, final Interval interval) {
        final Predicate<Item> validBroadcast = MorePredicates.transformingPredicate(ITEM_TO_BROADCAST_INTERVAL, new ScheduleBroadcastFilter(interval));
        
        return Iterables.transform(ImmutableSet.copyOf(Iterables.transform(Iterables.filter(items, validBroadcast), ItemScheduleEntry.ITEM_SCHEDULE_ENTRY)), ItemScheduleEntry.ITEM);
    }
    
    private List<Item> filterLocations(Iterable<Item> items) {
        return ImmutableList.copyOf(Iterables.transform(items, new Function<Item, Item>() {
            @Override
            public Item apply(Item input) {
                for (Version version: input.getVersions()) {
                    for (Encoding encoding: version.getManifestedAs()) {
                        if (! encoding.getAvailableAt().isEmpty()) {
                            encoding.setAvailableAt(ImmutableSet.copyOf(Iterables.filter(encoding.getAvailableAt(), Location.AVAILABLE_LOCATION)));
                        }
                    }
                }
                return input;
            }
        }));
    }
    
    private static List<String> keys(Iterable<Interval> intervals, Iterable<Channel> channels, Iterable<Publisher> publishers) {
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

	void writeScheduleFrom(Item item1) {
		writeScheduleFor(ImmutableList.of(item1));
	}
}
