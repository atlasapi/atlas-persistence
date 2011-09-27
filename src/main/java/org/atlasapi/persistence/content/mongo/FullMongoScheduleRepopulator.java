package org.atlasapi.persistence.content.mongo;

import static org.atlasapi.persistence.content.ContentTable.CHILD_ITEMS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_ITEMS;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;

import java.util.List;
import java.util.Map;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingHandler;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleEntryBuilder;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metabroadcast.common.scheduling.ScheduledTask;

public class FullMongoScheduleRepopulator extends ScheduledTask {
    
    private final ContentLister contentLister;
    private final ScheduleWriter scheduleStore;
    private final List<Publisher> publishers;
    private final ScheduleEntryBuilder scheduleEntryBuilder = new ScheduleEntryBuilder();

    public FullMongoScheduleRepopulator(ContentLister contentLister, ScheduleWriter scheduleStore, Iterable<Publisher> publishers) {
        this.contentLister = contentLister;
        this.scheduleStore = scheduleStore;
        this.publishers = ImmutableList.copyOf(publishers);
    }
    
    @Override
    public void runTask() {
        
        final Map<String, ScheduleEntry> scheduleEntries = Maps.newHashMap();
        
        contentLister.listContent(ImmutableSet.of(TOP_LEVEL_ITEMS, CHILD_ITEMS), defaultCriteria().forPublishers(publishers), new ContentListingHandler() {

            @Override
            public boolean handle(Iterable<? extends Content> contents, ContentListingProgress progress) {
                // Only looking at item tables, so all content will be items
                Map<String, ScheduleEntry> entries = scheduleEntryBuilder.toScheduleEntries(Iterables.filter(contents, Item.class));
                
                for (ScheduleEntry entry: entries.values()) {
                    ScheduleEntry existingEntry = scheduleEntries.get(entry.toKey());
                    if (existingEntry == null) {
                        scheduleEntries.put(entry.toKey(), entry);
                    } else {
                        existingEntry.withItems(Iterables.concat(existingEntry.getItemRefsAndBroadcasts(), entry.getItemRefsAndBroadcasts()));
                    }
                }
                reportStatus(progress.toString());
                return true;
            }
        });
        
        for (ScheduleEntry entry : scheduleEntries.values()) {
            scheduleStore.writeCompleteEntry(entry);
        }
    }
}
