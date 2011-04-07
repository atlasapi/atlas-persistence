package org.atlasapi.persistence.content.mongo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentListener;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;

import com.metabroadcast.common.concurrency.BoundedExecutor;

public class AsyncronousContentListener implements ContentListener {
    
    private final ContentListener delegate;
    private final ExecutorService executor = Executors.newFixedThreadPool(50);
    private final BoundedExecutor boundedQueue = new BoundedExecutor(executor, 100);
    private final AdapterLog log;
    
    public AsyncronousContentListener(ContentListener delegate, AdapterLog log) {
        this.delegate = delegate;
        this.log = log;
    }

    @Override
    public void brandChanged(Iterable<? extends Container<?>> container, ChangeType changeType) {
        try {
            boundedQueue.submitTask(new BrandChangedJob(container, changeType));
        } catch (Exception e) {
            log.record(new AdapterLogEntry(Severity.ERROR).withCause(e).withSource(AsyncronousContentListener.class));
        }
    }

    @Override
    public void itemChanged(Iterable<? extends Item> item, ChangeType changeType) {
        try {
            boundedQueue.submitTask(new ItemChangedJob(item, changeType));
        } catch (Exception e) {
            log.record(new AdapterLogEntry(Severity.ERROR).withCause(e).withSource(AsyncronousContentListener.class));
        }
    }

    class ItemChangedJob implements Runnable {
        
        private final Iterable<? extends Item> items;
        private final ChangeType changeType;

        public ItemChangedJob(Iterable<? extends Item> items, ChangeType changeType) {
            this.items = items;
            this.changeType = changeType;
        }

        @Override
        public void run() {
            try {
                delegate.itemChanged(items, changeType);
            } catch (Exception e) {
                log.record(new AdapterLogEntry(Severity.ERROR).withCause(e).withSource(AsyncronousContentListener.class));
            }
        }
    }
    
    class BrandChangedJob implements Runnable {
        
        private final Iterable<? extends Container<?>> containers;
        private final ChangeType changeType;

        public BrandChangedJob(Iterable<? extends Container<?>> containers, ChangeType changeType) {
            this.containers = containers;
            this.changeType = changeType;
        }

        @Override
        public void run() {
            try {
                delegate.brandChanged(containers, changeType);
            } catch (Exception e) {
                log.record(new AdapterLogEntry(Severity.ERROR).withCause(e).withSource(AsyncronousContentListener.class));
            }
        }
    }
}
