package org.atlasapi.persistence.content.mongo;

import static org.atlasapi.persistence.content.ContentTable.CHILD_ITEMS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_ITEMS;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.listing.ContentLister;
import org.atlasapi.persistence.content.listing.ContentListingHandler;
import org.atlasapi.persistence.content.listing.ContentListingProgress;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metabroadcast.common.scheduling.ScheduledTask;

public class FullMongoScheduleRepopulator extends ScheduledTask {
    
    private final ContentLister contentLister;
    private final ScheduleWriter scheduleStore;
    private final Iterable<Publisher> publishers;

    public FullMongoScheduleRepopulator(ContentLister contentLister, ScheduleWriter scheduleStore, Iterable<Publisher> forPublishers) {
        this.contentLister = contentLister;
        this.scheduleStore = scheduleStore;
        this.publishers = forPublishers;
    }
    
    @Override
    public void runTask() {
        
        final AtomicReference<List<Content>> batch = new AtomicReference<List<Content>>(Lists.<Content>newArrayList());
        
        contentLister.listContent(ImmutableSet.of(TOP_LEVEL_ITEMS, CHILD_ITEMS), defaultCriteria().forPublishers(publishers), new ContentListingHandler() {

            @Override
            public boolean handle(Content content, ContentListingProgress progress) {
                batch.get().add(content);
                if (batch.get().size() == 10) {
                    scheduleStore.writeScheduleFor(Iterables.filter(batch.getAndSet(Lists.<Content> newArrayList()), Item.class));
                }
                reportStatus(progress.toString());
                return true;
            }
        });
        
        scheduleStore.writeScheduleFor(Iterables.filter(batch.getAndSet(Lists.<Content>newArrayList()), Item.class));

    }
}
