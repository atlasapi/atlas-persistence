package org.atlasapi.persistence.content.mongo;

import static com.metabroadcast.common.persistence.mongo.MongoBuilders.where;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Container;
import org.atlasapi.persistence.media.entity.ContainerTranslator;

import com.google.common.collect.ImmutableList;
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
            for (DBObject dbObject: ImmutableList.copyOf(where().fieldGreaterThan(MongoConstants.ID, currentId).find(contentCollection, new MongoSortBuilder().ascending(MongoConstants.ID), BATCH_SIZE))) {
                Container<?> container = containerTranslator.fromDBObject(dbObject, null);
                try {
                    boundedQueue.submitTask(new UpdateItemScheduleJob(container));
                } catch (InterruptedException e) {
                    log.error("Problem submitting task to process queue for container: "+container, e);
                }
            }
        }
    }
    
    class UpdateItemScheduleJob implements Runnable {
        
        private final Container<?> container;

        public UpdateItemScheduleJob(Container<?> container) {
            this.container = container;
        }

        @Override
        public void run() {
            scheduleStore.createOrUpdate(container.getContents());
        }
    }
}
