package org.atlasapi.persistence.content.mongo;

import static org.atlasapi.persistence.content.ContentTable.CHILD_ITEMS;
import static org.atlasapi.persistence.content.ContentTable.TOP_LEVEL_ITEMS;
import static org.atlasapi.persistence.content.listing.ContentListingCriteria.defaultCriteria;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.metabroadcast.common.concurrency.BoundedExecutor;
import com.metabroadcast.common.scheduling.ScheduledTask;

public class FullMongoScheduleRepopulator extends ScheduledTask {
    
    private static final Log log = LogFactory.getLog(FullMongoScheduleRepopulator.class);

    private final ContentLister contentLister;
    private final ScheduleWriter scheduleStore;
    private final Iterable<Publisher> publishers;

    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final BoundedExecutor boundedQueue = new BoundedExecutor(executor, 10);

    public FullMongoScheduleRepopulator(ContentLister contentLister, ScheduleWriter scheduleStore, Iterable<Publisher> forPublishers) {
        this.contentLister = contentLister;
        this.scheduleStore = scheduleStore;
        this.publishers = forPublishers;
    }
    
    @Override
    public void runTask() {
        
        final AtomicReference<List<Content>> batch = new AtomicReference<List<Content>>(Lists.<Content>newArrayList());
        ContentListingHandler handler = new ContentListingHandler() {
            
            @Override
            public boolean handle(Content content, ContentListingProgress progress) {
                if(batch.get().size() < 10) {
                    batch.get().add(content);
                } else {
                    try {
                        Iterable<Item> itemBatch = Iterables.filter(batch.getAndSet(Lists.<Content>newArrayList()), Item.class);
                        boundedQueue.submitTask(new UpdateItemScheduleJob(itemBatch));
                    } catch (InterruptedException e) {
                        log.error("Problem submitting task to process queue for items: " + batch, e);
                    }
                }
                reportStatus(String.format("%s / %s items", progress.count(), progress.total()));
                return true;
            }
        };
        
        contentLister.listContent(ImmutableSet.of(TOP_LEVEL_ITEMS, CHILD_ITEMS), defaultCriteria().forPublishers(publishers), handler );
        
        try {
            boundedQueue.submitTask(new UpdateItemScheduleJob(Iterables.filter(batch.get(), Item.class)));
        } catch (InterruptedException e) {
            log.error("Problem submitting task to process queue for items: " + batch, e);
        }
    }
    
    class UpdateItemScheduleJob implements Runnable {
        
        private final Iterable<? extends Item> items;

        public UpdateItemScheduleJob(Iterable<? extends Item> items) {
            this.items = items;
        }

        @Override
        public void run() {
            scheduleStore.writeScheduleFor(items);
        }
    }
}
