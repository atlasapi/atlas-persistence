package org.atlasapi.persistence.content.mongo;

import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.persistence.content.ContentCategory;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleEntryBuilder;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.metabroadcast.common.scheduling.ScheduledTask;

public class FullMongoScheduleRepopulator extends ScheduledTask {
    
    private final ContentLister contentLister;
    private final ScheduleWriter scheduleStore;
    private final List<Publisher> publishers;
    private final ScheduleEntryBuilder scheduleEntryBuilder = new ScheduleEntryBuilder();
    private int lastProcessed = 0;

    public FullMongoScheduleRepopulator(ContentLister contentLister, ScheduleWriter scheduleStore, Iterable<Publisher> publishers) {
        this.contentLister = contentLister;
        this.scheduleStore = scheduleStore;
        this.publishers = ImmutableList.copyOf(publishers);
    }
    
    @Override
    public void runTask() {
        
        final Map<String, ScheduleEntry> scheduleEntries = Maps.newHashMap();
        int processed = 0;

        Iterator<Content> items = contentLister.listContent(defaultCriteria().forContent(ImmutableList.copyOf(ContentCategory.ITEMS)).forPublishers(publishers).build());
        Iterator<List<Content>> itemLists = Iterators.partition(items, 100);
        
        while (itemLists.hasNext()) {
            Map<String, ScheduleEntry> entries = scheduleEntryBuilder.toScheduleEntries(Iterables.filter(itemLists.next(), Item.class));

            for (ScheduleEntry entry : entries.values()) {
                ScheduleEntry existingEntry = scheduleEntries.get(entry.toKey());
                if (existingEntry == null) {
                    scheduleEntries.put(entry.toKey(), entry);
                } else {
                    existingEntry.withItems(Iterables.concat(existingEntry.getItemRefsAndBroadcasts(), entry.getItemRefsAndBroadcasts()));
                }
                
                reportStatus(String.format("Building schedule entries. Processed %s (%s)", processed, lastProcessed));
            }
            
        }
        

        reportStatus(String.format("Writing %s schedule entries", scheduleEntries.values().size()));
        
        for (ScheduleEntry entry : scheduleEntries.values()) {
            scheduleStore.writeCompleteEntry(entry);
        }

        reportStatus(String.format("Wrote %s schedule entries for %s content", scheduleEntries.values().size(), processed));
        lastProcessed  = processed;
    }
}
