package org.atlasapi.persistence.content.mongo;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.content.ContentListener;

public class AsyncronousContentListener implements ContentListener {
    
    private final Log log = LogFactory.getLog(AsyncronousContentListener.class);
    private final ContentListener delegate;
    private final ExecutorService executor;
    
    public AsyncronousContentListener(ContentListener delegate) {
        this(delegate, Executors.newCachedThreadPool());
    }

    public AsyncronousContentListener(ContentListener delegate, ExecutorService executor) {
        this.delegate = delegate;
        this.executor = executor;
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
                log.error(e);
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
                log.error(e);
            }
        }
    }
}
