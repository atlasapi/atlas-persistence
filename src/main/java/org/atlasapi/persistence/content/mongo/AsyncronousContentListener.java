package org.atlasapi.persistence.content.mongo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentListener;
import org.atlasapi.persistence.logging.AdapterLog;
import org.atlasapi.persistence.logging.AdapterLogEntry;
import org.atlasapi.persistence.logging.AdapterLogEntry.Severity;

public class AsyncronousContentListener implements ContentListener {
    
    private final ContentListener delegate;
    private final ExecutorService executor;
    private final AdapterLog log;
    
    public AsyncronousContentListener(ContentListener delegate, AdapterLog log) {
        this(delegate, Executors.newCachedThreadPool(), log);
    }

    public AsyncronousContentListener(ContentListener delegate, ExecutorService executor, AdapterLog log) {
        this.delegate = delegate;
        this.executor = executor;
        this.log = log;
    }

    @Override
    public void brandChanged(Iterable<? extends Container<?>> container, ChangeType changeType) {
        executor.execute(new BrandChangedJob(container, changeType));
    }

    @Override
    public void itemChanged(Iterable<? extends Item> item, ChangeType changeType) {
        executor.execute(new ItemChangedJob(item, changeType));
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
