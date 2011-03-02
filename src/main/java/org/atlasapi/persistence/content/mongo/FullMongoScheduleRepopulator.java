package org.atlasapi.persistence.content.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.media.entity.ContainerTranslator;
import org.atlasapi.persistence.media.entity.ItemTranslator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.concurrency.BoundedExecutor;
import com.metabroadcast.common.persistence.mongo.DatabasedMongo;
import com.metabroadcast.common.persistence.mongo.MongoConstants;
import com.metabroadcast.common.persistence.mongo.MongoSortBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class FullMongoScheduleRepopulator implements Runnable {
    
    private final MongoScheduleStore scheduleStore;
    private final DBCollection contentCollection;
    private static final Log log = LogFactory.getLog(FullMongoScheduleRepopulator.class);
    private static final int BATCH_SIZE = 100;
    private final ContainerTranslator containerTranslator = new ContainerTranslator();
    private final ItemTranslator itemTranslator = new ItemTranslator(true);
    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final BoundedExecutor boundedQueue = new BoundedExecutor(executor, 10);

    public FullMongoScheduleRepopulator(DatabasedMongo db, MongoScheduleStore scheduleStore) {
        contentCollection = db.collection("content");
        this.scheduleStore = scheduleStore;
    }

    @Override
    public void run() {
        String currentId = "0";
        
        while (true) {
            List<DBObject> objects = ImmutableList.copyOf(where().fieldGreaterThan(MongoConstants.ID, currentId).find(contentCollection, new MongoSortBuilder().ascending(MongoConstants.ID), BATCH_SIZE));
            if (objects.isEmpty()) {
                continue;
            }
            
            for (DBObject dbObject: objects) {
                String type = (String) dbObject.get("type");
                List<? extends Item> items = ImmutableList.of();
                if (Episode.class.getSimpleName().equals(type) || Clip.class.getSimpleName().equals(type) || Item.class.getSimpleName().equals(type)) {
                    items = ImmutableList.of(itemTranslator.fromDBObject(dbObject, null));
                } else {
                    Container<?> container = containerTranslator.fromDBObject(dbObject, null);
                    items = container.getContents();
                }
                
                String lastestId = items.isEmpty() ? null : Iterables.getLast(items).getCanonicalUri();
                if (items == null || currentId.equals(lastestId)) {
                    continue;
                }
                currentId = lastestId;
                try {
                    boundedQueue.submitTask(new UpdateItemScheduleJob(items));
                } catch (InterruptedException e) {
                    log.error("Problem submitting task to process queue for items: "+items, e);
                }
            }
        }
    }
    
    class UpdateItemScheduleJob implements Runnable {
        
        private final Iterable<? extends Item> items;

        public UpdateItemScheduleJob(Iterable<? extends Item> items) {
            this.items = items;
        }

        @Override
        public void run() {
            scheduleStore.createOrUpdate(items);
        }
    }
}
