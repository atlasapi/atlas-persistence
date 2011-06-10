package org.atlasapi.persistence.content.mongo;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.schedule.mongo.ScheduleWriter;
import org.atlasapi.persistence.media.entity.ItemTranslator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.concurrency.BoundedExecutor;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoBuilders;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoQueryBuilder;
import com.metabroadcast.common.persistence.mongo.MongoSortBuilder;
import com.metabroadcast.common.persistence.translator.TranslatorUtils;
import com.metabroadcast.common.scheduling.ScheduledTask;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class FullMongoScheduleRepopulator extends ScheduledTask {
    
    private static final Log log = LogFactory.getLog(FullMongoScheduleRepopulator.class);
    private static final int BATCH_SIZE = 5;

    private final DBCollection childrenCollection;
    private final DBCollection topLevelItemCollection;
    private final ScheduleWriter scheduleStore;
    private final Iterable<Publisher> publishers;

    private final ItemTranslator itemTranslator = new ItemTranslator();
    
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final BoundedExecutor boundedQueue = new BoundedExecutor(executor, 10);

    public FullMongoScheduleRepopulator(DatabasedMongo db, ScheduleWriter scheduleStore, Iterable<Publisher> forPublishers) {
        this.childrenCollection = db.collection("children");
        this.topLevelItemCollection = db.collection("topLevelItems");
        this.scheduleStore = scheduleStore;
        this.publishers = forPublishers;
    }
    
    private MongoQueryBuilder where(Iterable<Publisher> forPublishers) {
        if (Iterables.isEmpty(forPublishers)) {
            return MongoBuilders.where();
        }
        return MongoBuilders.where().fieldIn("publisher", Iterables.transform(forPublishers, Publisher.TO_KEY));
    }
    
    @Override
    public void runTask() {
        String currentId = "0";
        long totalRows = countItems();
        long rowsSeen = 0;
        long errors = 0;
        
        for(int i = 0; i < 2 && shouldContinue(); i++) {
            DBCollection collection = ImmutableList.of(childrenCollection,topLevelItemCollection).get(i);
            while (shouldContinue()) {
                reportStatus(rowsSeen + "/" + totalRows + ", " + errors + " errors");

                List<DBObject> objects = ImmutableList.copyOf(where(publishers).fieldGreaterThan(MongoConstants.ID, currentId).find(collection, new MongoSortBuilder().ascending(MongoConstants.ID),
                        -BATCH_SIZE));
                if (objects.isEmpty()) {
                    break;
                }
                rowsSeen += objects.size();

                final String latestId = TranslatorUtils.toString(Iterables.getLast(objects), MongoConstants.ID);
                if (latestId == null || latestId.equals(currentId)) {
                    break;
                }
                currentId = latestId;

                List<Item> items = toItems(objects);
                errors += objects.size() - items.size();

                try {
                    boundedQueue.submitTask(new UpdateItemScheduleJob(items));
                } catch (InterruptedException e) {
                    log.error("Problem submitting task to process queue for items: " + items, e);
                }
            }
        }
    }

    private List<Item> toItems(List<DBObject> objects) {
        ImmutableList.Builder<Item> itemsBuilder = ImmutableList.builder();
        for (DBObject dbObject : objects) {
            try {
                itemsBuilder.add(itemTranslator.fromDBObject(dbObject, null));
            } catch (Exception e) {
                log.error("Problem translating content from mongo: " + TranslatorUtils.toString(dbObject, MongoConstants.ID), e);
            }
        }
        return itemsBuilder.build();
    }

    private long countItems() {
        return childrenCollection.count(where(publishers).build()) + topLevelItemCollection.count(where(publishers).build());
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
