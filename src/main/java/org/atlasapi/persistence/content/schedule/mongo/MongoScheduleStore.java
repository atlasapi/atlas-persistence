package org.atlasapi.persistence.content.schedule.mongo;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.metabroadcast.applications.client.model.internal.Application;
import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.base.MorePredicates;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.SystemClock;
import com.metabroadcast.common.time.Timestamp;
import com.metabroadcast.common.time.Timestamper;
import com.mongodb.DBCollection;
import org.atlasapi.application.v3.DefaultApplication;
import org.atlasapi.equiv.OutputContentMerger;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
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
import org.atlasapi.messaging.v3.ScheduleUpdateMessage;
import org.atlasapi.output.Annotation;
import org.atlasapi.persistence.content.ContentResolver;
import org.atlasapi.persistence.content.ContentWriter;
import org.atlasapi.persistence.content.EquivalentContent;
import org.atlasapi.persistence.content.EquivalentContentResolver;
import org.atlasapi.persistence.content.ResolvedContent;
import org.atlasapi.persistence.content.ScheduleResolver;
import org.atlasapi.persistence.content.schedule.ScheduleBroadcastFilter;
import org.atlasapi.persistence.media.entity.ScheduleEntryTranslator;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.format.PeriodFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

public class MongoScheduleStore implements ScheduleResolver, ScheduleWriter {

	public static final Duration MAX_DURATION = Duration.standardDays(14);
    public static final int MAX_ALLOWED_YEAR = 2100;

	private final ScheduleEntryBuilder scheduleEntryBuilder;
    private final DBCollection collection;
    private final ScheduleEntryTranslator translator;

    private final EquivalentContentResolver equivalentContentResolver;
    private final ChannelResolver channelResolver;

    private final ContentWriter contentWriter;

    private final MessageSender<ScheduleUpdateMessage> messageSender;
    private final Timestamper timestamper = new SystemClock();
    private final NumberToShortStringCodec idCodec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private final Logger log = LoggerFactory.getLogger(getClass());

    public MongoScheduleStore(
            DatabasedMongo db,
            ChannelResolver channelResolver,
            ContentResolver contentResolver,
            ContentWriter contentWriter,
            EquivalentContentResolver equivalentContentResolver,
            MessageSender<ScheduleUpdateMessage> messageSender
    ) {
        this.channelResolver = channelResolver;
        this.contentResolver = contentResolver;
        this.contentWriter = contentWriter;
        this.equivalentContentResolver = equivalentContentResolver;
        collection = db.collection("schedule");
        this.scheduleEntryBuilder = new ScheduleEntryBuilder(
                channelResolver,
                Duration.standardSeconds(Long.MAX_VALUE/1000)
        );
        translator = new ScheduleEntryTranslator(channelResolver);
        this.messageSender = messageSender;
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
    public void replaceScheduleBlock(
            Publisher publisher,
            Channel channel,
            Iterable<ItemRefAndBroadcast> itemsAndBroadcasts
    ) {
        Interval interval = checkAndGetScheduleInterval(itemsAndBroadcasts, true, channel);
        Map<String, ScheduleEntry> entries = getAdjacentScheduleEntries(
                channel,
                publisher,
                interval
        );
        
        for(ItemRefAndBroadcast itemAndBroadcast : itemsAndBroadcasts) {
            scheduleEntryBuilder.toScheduleEntryFromBroadcast(channel, publisher, itemAndBroadcast, entries);
        }
        for (ScheduleEntry entry : entries.values()) {
            writeCompleteEntry(entry);
        }
        
        sendUpdateMessage(publisher, channel, interval);
    }
    
    private void sendUpdateMessage(Publisher publisher, Channel channel, Interval interval) {
        String mid = UUID.randomUUID().toString();
        Timestamp ts = timestamper.timestamp();
        String src = publisher.key();
        String cid = idCodec.encode(BigInteger.valueOf(channel.getId()));
        DateTime start = interval.getStart();
        DateTime end = interval.getEnd();
        ScheduleUpdateMessage msg = new ScheduleUpdateMessage(mid, ts, src, cid , start, end);
        try {
            messageSender.sendMessage(msg, msg.getChannel().getBytes());
        } catch (MessagingException e) {
            String errMsg = String.format("%s %s %s", src, cid, interval);
            log.error(errMsg, e);
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
		
		Broadcast previousBroadcast = null;
		for(Broadcast b : broadcasts) {
			
			if(!expectedChannel.equals(channelResolver.fromUri(b.getBroadcastOn()).requireValue())) {
				throw new IllegalArgumentException("All broadcasts must be on the same channel; "
				                + scheduleDebugInfo(previousBroadcast, b));
			}	
			
			if(allowGaps) {
				if(currentEndTime != null && b.getTransmissionTime().isBefore(currentEndTime)) {
					throw new IllegalArgumentException("Overlapping periods found in schedule " 
					            + scheduleDebugInfo(previousBroadcast, b));
				}
			}
			else {
				if(currentEndTime != null && !(currentEndTime.equals(b.getTransmissionTime()))) {
					throw new IllegalArgumentException("Schedule is not contiguous; " 
				+ scheduleDebugInfo(previousBroadcast, b));
				}
			}
			currentEndTime = b.getTransmissionEndTime();
			previousBroadcast = b;
		}
		
		return new Interval(broadcasts.get(0).getTransmissionTime(), currentEndTime);
    }
   
    private String scheduleDebugInfo(Broadcast previous, Broadcast current) {
        if (previous == null) {
            return String.format("On first broadcast, ID %s, starts %s", current.getSourceId(), current.getTransmissionTime());
        }
        
        return String.format("Last broadcast ID %s, ends %s; next broadcast ID %s, starts %s", 
                previous.getSourceId(), previous.getTransmissionEndTime().toString(),
                current.getSourceId(), current.getTransmissionTime().toString());
    }
    
	private Map<String, ScheduleEntry> getAdjacentScheduleEntries(
            Channel channel,
            Publisher publisher,
            final Interval interval
    ) {
        List<Interval> intervals = scheduleEntryBuilder.intervalsFor(interval.getStart(), interval.getEnd()); 
        Set<String> requiredScheduleEntries = Sets.<String>newHashSet();

	    String firstScheduleEntryKey = ScheduleEntry.toKey(intervals.get(0), channel, publisher);
	    String lastScheduleEntryKey = ScheduleEntry.toKey(intervals.get(intervals.size() - 1), channel, publisher);
	        
	    requiredScheduleEntries.add(firstScheduleEntryKey);
	    requiredScheduleEntries.add(lastScheduleEntryKey);
	    Map<String, ScheduleEntry> scheduleEntries = Maps.newHashMap(Maps.uniqueIndex(translator.fromDbObjects(where().idIn(requiredScheduleEntries).find(collection)), ScheduleEntry.KEY));

        Predicate<ItemRefAndBroadcast> beforePredicate = i -> i.getBroadcast()
                .getTransmissionEndTime()
                .isBefore(interval.getStart());

        Predicate<ItemRefAndBroadcast> onBeforePredicate = i -> i.getBroadcast()
                .getTransmissionEndTime()
                .isEqual(interval.getStart());

        Predicate<ItemRefAndBroadcast> onAfterPredicate = i -> i.getBroadcast()
                .getTransmissionTime()
                .isEqual(interval.getEnd());

        Predicate<ItemRefAndBroadcast> afterPredicate = i -> i.getBroadcast()
                .getTransmissionTime()
                .isAfter(interval.getEnd());

        // If the first and last period are identical,
        // retain items that start before the interval or start after the end of the interval
        if(firstScheduleEntryKey.equals(lastScheduleEntryKey)
                && scheduleEntries.containsKey(firstScheduleEntryKey)){
            ScheduleEntry firstEntry = filteredScheduleEntry(
                    scheduleEntries.get(firstScheduleEntryKey),
                    Predicates.or(onBeforePredicate, beforePredicate, onAfterPredicate, afterPredicate)
            );
            scheduleEntries.put(firstScheduleEntryKey, firstEntry);
            return scheduleEntries;
        }

	    // For the first period, retain items that start before the interval 
	    if(scheduleEntries.containsKey(firstScheduleEntryKey)) {
            ScheduleEntry firstEntry = filteredScheduleEntry(
		            scheduleEntries.get(firstScheduleEntryKey),
                    Predicates.or(onBeforePredicate, beforePredicate)
            );
		    scheduleEntries.put(firstScheduleEntryKey, firstEntry);
	    }
	  
	    // For the last period, retain items that start on or after the end of the interval
	    if(scheduleEntries.containsKey(lastScheduleEntryKey)) {

            ScheduleEntry lastEntry = filteredScheduleEntry(
		            scheduleEntries.get(lastScheduleEntryKey),
                    Predicates.or(onAfterPredicate, afterPredicate)
            );
		    scheduleEntries.put(lastScheduleEntryKey, lastEntry);
	    }
	    return scheduleEntries;
	}

    private ScheduleEntry filteredScheduleEntry(
            ScheduleEntry entry,
            Predicate<ItemRefAndBroadcast> filterPredicate
    )  {
        Iterable<ItemRefAndBroadcast> filteredBroadcasts = Collections2.filter(
                entry.getItemRefsAndBroadcasts(),
                filterPredicate
        );
        ScheduleEntry filteredEntry = new ScheduleEntry(
                entry.interval(),
                entry.channel(),
                entry.publisher(),
                filteredBroadcasts
        );
		return filteredEntry;
    }

    @Override
	public Schedule unmergedSchedule(DateTime from, DateTime to, Iterable<Channel> channels,
	        Iterable<Publisher> publishers) {

        Interval interval = getInterval(from,  to);
        List<ScheduleEntry> entries = resolveEntries(channels, from, to, publishers);
        Iterable<Entry<Channel, ItemRefAndBroadcast>> uniqueRefs = uniqueRefs(entries);
        Map<String, Maybe<Identified>> itemIndex = resolveItems(uniqueRefs);
	    return scheduleFrom(toChannelMap(channels, uniqueRefs, itemIndex), interval);
	}

	@Override
    public Set<Item> resolveItems(
            DateTime from,
            DateTime to,
            Iterable<Channel> channels,
            Iterable<Publisher> publishers
    ) {
        List<ScheduleEntry> entries = resolveEntries(channels, from, to, publishers);
        Iterable<Entry<Channel, ItemRefAndBroadcast>> uniqueRefs = uniqueRefs(entries);
        Map<String, Maybe<Identified>> itemIndex = resolveItems(uniqueRefs);
        return itemIndex.values().stream()
                .filter(Maybe::hasValue)
                .map(Maybe::requireValue)
                .filter(Item.class::isInstance)
                .map(Item.class::cast)
                .collect(MoreCollectors.toImmutableSet());
    }
	
    private Map<String, Maybe<Identified>> resolveItems(
            Iterable<Entry<Channel, ItemRefAndBroadcast>> refs) {
        ImmutableSet<String> uris = uniqueUris(refs);
        ResolvedContent resolved = contentResolver.findByCanonicalUris(uris);
        Builder<String, Maybe<Identified>> itemIndex = ImmutableMap.builder();
        for (String uri : uris) {
            itemIndex.put(uri, resolved.get(uri));
        }
        return itemIndex.build();
    }

    @Override
    public Schedule schedule(
            DateTime from,
            DateTime to,
            Iterable<Channel> channels,
            Iterable<Publisher> publishers,
            Optional<Application> applicationOptional
    ) {
        Interval interval = getInterval(from, to);
        List<ScheduleEntry> entries = resolveEntries(channels, from, to, publishers);

        Application application;
        if(applicationOptional.isPresent()) {
            application = applicationOptional.get();
        } else {
            application = configFor(publishers);
        }
        Map<Channel, List<Item>> channelMap = resolveEntries(entries, channels, application);
        return scheduleFrom(channelMap, interval);
    }

    private Interval getInterval(DateTime from, DateTime to){
        //if the date is mistakenly given as 20170808, it will parsed to a normal date for the year
        //20170808. This will pass the normal interval test below, but it does not convert normally
        //to milliseconds which is how we use this, resulting in requesting a couple of million
        //years and an out of memory error.
        if (from.getYear() > MAX_ALLOWED_YEAR || to.getYear() > MAX_ALLOWED_YEAR) {
            throw new IllegalArgumentException("You cannot request schedule for after year "+MAX_ALLOWED_YEAR);
        }
        Interval interval = new Interval(from, to);
        if (interval.toDuration().isLongerThan(MAX_DURATION)) {
            throw new IllegalArgumentException("You cannot request more than "
                                               + PeriodFormat.getDefault().print(MAX_DURATION.toPeriod())
                                               + " of schedule");
        }
        return interval;
    }

    private Application configFor(Iterable<Publisher> publishers) {
        return DefaultApplication.createWithReads(Lists.newArrayList(publishers));
    }

    private Map<Channel, List<Item>> resolveEntries(
            Iterable<ScheduleEntry> entries,
            Iterable<Channel> channels,
            Application application
    ) {
        // TODO this code inefficient, but in future we should avoid hydrating then items
        // unless explicitly requested
        Iterable<Entry<Channel, ItemRefAndBroadcast>> uniqueRefs = uniqueRefs(entries);
        Map<String, Maybe<Identified>> itemIndex = resolveItems(uniqueRefs, application);
        return toChannelMap(channels, uniqueRefs, itemIndex);
    }

    private Map<Channel, List<Item>> toChannelMap(Iterable<Channel> channels,
            Iterable<Entry<Channel, ItemRefAndBroadcast>> uniqueRefs,
            Map<String, Maybe<Identified>> itemIndex) {
        SetMultimap<String, Broadcast> broadcastIndex = indexBroadcasts(itemIndex);
        itemIndex = removeBroadcasts(itemIndex);
        Map<Channel, List<Item>> channelMap = createChannelMap(channels);
        for (Entry<Channel,ItemRefAndBroadcast> entry : uniqueRefs) {
            String itemUri = entry.getValue().getItemUri();
            final Maybe<Identified> possibleItem = itemIndex.get(itemUri);
            if (possibleItem.hasValue()) {
                Item item = ((Item) possibleItem.requireValue()).copy();
                if (selectAndTrimBroadcast(broadcastIndex.get(itemUri), item, entry.getValue().getBroadcast())) {
                    channelMap.get(entry.getKey()).add(item);
                }
            }
        }
        return channelMap;
    }
    
    private SetMultimap<String, Broadcast> indexBroadcasts(Map<String, Maybe<Identified>> itemIndex) {
        ImmutableSetMultimap.Builder<String, Broadcast> broadcasts = ImmutableSetMultimap.builder();
        for (Entry<String, Maybe<Identified>> itemEntry : itemIndex.entrySet()) {
            if (itemEntry.getValue().hasValue() && itemEntry.getValue().requireValue() instanceof Item) {
                for (Version version : ((Item)itemEntry.getValue().requireValue()).getVersions()) {
                    broadcasts.putAll(itemEntry.getKey(), version.getBroadcasts());
                }
            }
        }
        return broadcasts.build();
    }

    private Map<String, Maybe<Identified>> removeBroadcasts(Map<String, Maybe<Identified>> itemIndex) {
        for (Maybe<Identified> possibleItem : itemIndex.values()) {
            if (possibleItem.hasValue() && possibleItem.requireValue() instanceof Item) {
                removeBroadcasts((Item)possibleItem.requireValue());
            }
        }
        return itemIndex;
    }

    private void removeBroadcasts(Item item) {
        for (Version version : item.getVersions()) {
            log.trace("Removing {} broadcasts from item {}", version.getBroadcasts().size(), item.getCanonicalUri());
            version.setBroadcasts(ImmutableSet.<Broadcast>of());
        }
    }

    private Map<String, Maybe<Identified>> resolveItems(
            Iterable<Entry<Channel, ItemRefAndBroadcast>> refs,
            Application mergeApplication
    ) {
        return findAndMerge(mergeApplication, refs);
    }

    private ImmutableSet<String> uniqueUris(Iterable<Entry<Channel, ItemRefAndBroadcast>> refs) {
        return ImmutableSet.copyOf(Iterables.transform(
                refs,
                input -> input.getValue().getItemUri()
        ));
    }

    private Map<String, Maybe<Identified>> findAndMerge(Application mergeApplication,
            Iterable<Entry<Channel, ItemRefAndBroadcast>> refs) {
        ImmutableSet<String> uris = uniqueUris(refs);
        EquivalentContent resolved = equivalentContentResolver.resolveUris(
                uris,
                mergeApplication,
                Annotation.defaultAnnotations(),
                false
        );
        ImmutableMap.Builder<String, Maybe<Identified>> result = ImmutableMap.builder();
        for (String uri : uris) {
            Set<Content> equivalents = resolved.get(uri);
            if (equivalents == null || equivalents.isEmpty()) {
                result.put(uri, Maybe.<Identified>nothing());
            } else {
                //ensure the relevant schedule item is at the head of the Content
                // list so it's selected as The Chosen One when merging equivalents
                // and will bring balance to The Force.
                ImmutableList<Content> equivList = ImmutableSet.<Content>builder()
                    .add(find(uri, equivalents))
                    .addAll(equivalents).build()
                    .asList();
                result.put(uri, Maybe.<Identified>just(merger.merge(mergeApplication, equivList).get(0)));
            }
        }
        return result.build();
    }

    private Iterable<Entry<Channel,ItemRefAndBroadcast>> uniqueRefs(Iterable<ScheduleEntry> entries) {
        ImmutableSet.Builder<Entry<Channel, ItemRefAndBroadcast>> refs = ImmutableSet.builder();
        for (ScheduleEntry scheduleEntry : entries) {
            for (ItemRefAndBroadcast entryRef : scheduleEntry.getItemRefsAndBroadcasts()) {
                refs.add(Maps.immutableEntry(scheduleEntry.channel(), entryRef));
            }
        }
        return refs.build();
    }

    /*
     * To resolve a schedule by count we need to guess how many hour buckets we
     * need to fulfill the count. We start off by resolving a set window and
     * counting how many unique item/broadcasts we got. While we don't have enough
     * we repeatedly resolve the next window.
     */
    @Override
    public Schedule schedule(
            final DateTime from,
            int count,
            Iterable<Channel> channels,
            Iterable<Publisher> publishers,
            Optional<Application> applicationOptional
    ) {

        DateTime start = from;
        DateTime end = from;
        final Duration windowDuration = Duration.standardDays(1);

        Map<Channel, ScheduleEntry> entries = Maps.newHashMap();
        
        boolean reachedCount = false;
        final int maxIterations = 2*count;
        int iterations = 0;
        Set<Channel> channelsNeedingMore = Sets.newHashSet(channels);
        
        while(!reachedCount && iterations < maxIterations) {
            start = end;
            end = start.plus(windowDuration);
            List<ScheduleEntry> windowEntries = resolveEntries(channelsNeedingMore, start, end, publishers);
            entries = merge(from, count, entries, windowEntries);
            
            for (ScheduleEntry scheduleEntry : entries.values()) {
                if (scheduleEntry.getItemRefsAndBroadcasts().size() >= count) {
                    channelsNeedingMore.remove(scheduleEntry.channel());
                }
            }
            
            reachedCount = channelsNeedingMore.isEmpty();
            iterations++;
        }

        Application application;
        if (applicationOptional.isPresent()) {
            application = applicationOptional.get();
        } else {
            application = configFor(publishers);
        }

        Map<Channel, List<Item>> channelMap = resolveEntries(entries.values(), channels, application);
        return scheduleFrom(channelMap, new Interval(from, end));
    }

    private Schedule scheduleFrom(Map<Channel, List<Item>> channelMap, Interval interval) {
        ImmutableMap.Builder<Channel, List<Item>> processedChannelMap = ImmutableMap.builder();
        for (Entry<Channel, List<Item>> entry: channelMap.entrySet()) {
            processedChannelMap.put(entry.getKey(), processChannelItems(entry.getValue(), interval));
        }
        return Schedule.fromChannelMap(processedChannelMap.build(), interval);
    }

    private List<ScheduleEntry> resolveEntries(Iterable<Channel> channels, DateTime start, DateTime end,
            Iterable<Publisher> publishers) {
        List<String> keys = keys(scheduleEntryBuilder.intervalsFor(start, end), channels, publishers);
        return translator.fromDbObjects(where().idIn(keys).find(collection));
    }

    private Map<Channel, ScheduleEntry> merge(
            DateTime from,
            int count,
            Map<Channel, ScheduleEntry> existing,
            List<ScheduleEntry> fetched
    ) {
        for (ScheduleEntry scheduleEntry : fetched) {
            ScheduleEntry existingEntry = existing.get(scheduleEntry.channel());
            if (existingEntry != null) {
                scheduleEntry = mergeScheduleEntries(existingEntry, scheduleEntry);
            } else {
                scheduleEntry = after(from,  scheduleEntry);
            }
            existing.put(scheduleEntry.channel(), limitEntryItems(count, scheduleEntry));
        }
        return existing;
    }

    private ScheduleEntry after(final DateTime from, ScheduleEntry entry) {
        return entry.withItems(Iterables.filter(entry.getItemRefsAndBroadcasts(),
                input -> input.getBroadcast().getTransmissionEndTime().isAfter(from)
        ));
    }

    private ScheduleEntry limitEntryItems(int count, ScheduleEntry scheduleEntry) {
        return scheduleEntry.withItems(Iterables.limit(scheduleEntry.getItemRefsAndBroadcasts(), count));
    }

    private ScheduleEntry mergeScheduleEntries(ScheduleEntry existing, ScheduleEntry fetched) {
        return existing.withItems(Iterables.concat(existing.getItemRefsAndBroadcasts(), fetched.getItemRefsAndBroadcasts()));
    }

    private Content find(String uri, Set<Content> equivalents) {
        for (Content content : equivalents) {
            if (uri.equals(content.getCanonicalUri())) {
                return content;
            }
        }
        throw new IllegalStateException(uri + " not found in its own equivalent content set");
    }

    private boolean selectAndTrimBroadcast(Set<Broadcast> broadcasts, Item item, Broadcast scheduleBroadcast) {
        boolean found = false;
        for (Broadcast broadcast : broadcasts) {
            if (scheduleBroadcast.equals(broadcast) && broadcast.isActivelyPublished()) {
                addBroadcast(item, broadcast);
                found = true;
            }
        }
        return found;
    }
    
    private void addBroadcast(Item item, Broadcast broadcast) {
        item.getVersions().iterator().next().setBroadcasts(ImmutableSet.of(broadcast));
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
        return Ordering.from(ScheduleEntry.START_TIME_AND_DURATION_ITEM_COMPARATOR)
                .immutableSortedCopy(items);
    }
    
    private static final Function<Broadcast, Interval> TO_BROADCAST = input ->
            new Interval(input.getTransmissionTime(), input.getTransmissionEndTime());

    private static final Function<Item, Interval> ITEM_TO_BROADCAST_INTERVAL = Functions.compose(
            TO_BROADCAST,
            ScheduleEntry.BROADCAST
    );

    private final OutputContentMerger merger = new OutputContentMerger();

    private final ContentResolver contentResolver;
    
    private Iterable<Item> filterItems(Iterable<Item> items, final Interval interval) {
        final Predicate<Item> validBroadcast = MorePredicates.transformingPredicate(
                ITEM_TO_BROADCAST_INTERVAL,
                new ScheduleBroadcastFilter(interval)
        );
        
        return Iterables.transform(
                ImmutableSet.copyOf(Iterables.transform(
                        Iterables.filter(items, validBroadcast),
                        ItemScheduleEntry.ITEM_SCHEDULE_ENTRY
                        )
                ),
                ItemScheduleEntry.ITEM
        );
    }
    
    private List<Item> filterLocations(Iterable<Item> items) {
        return ImmutableList.copyOf(Iterables.transform(items, input -> {
            for (Version version: input.getVersions()) {
                for (Encoding encoding: version.getManifestedAs()) {
                    if (! encoding.getAvailableAt().isEmpty()) {
                        encoding.setAvailableAt(ImmutableSet.copyOf(Iterables.filter(
                                encoding.getAvailableAt(),
                                Location.AVAILABLE_LOCATION))
                        );
                    }
                }
            }
            return input;
        }));
    }
    
    private static List<String> keys(
            Iterable<Interval> intervals,
            Iterable<Channel> channels,
            Iterable<Publisher> publishers
    ) {
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
                return startTime().equals(entry.startTime())
                        && canonicalUri().equals(entry.canonicalUri()
                );
            }
            return super.equals(obj);
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(startTime(), canonicalUri());
        }
        
        final static Function<ItemScheduleEntry, Item> ITEM = ItemScheduleEntry::item;
        
        final static Function<Item, ItemScheduleEntry> ITEM_SCHEDULE_ENTRY = ItemScheduleEntry::new;
    }

	void writeScheduleFrom(Item item1) {
		writeScheduleFor(ImmutableList.of(item1));
	}
}
