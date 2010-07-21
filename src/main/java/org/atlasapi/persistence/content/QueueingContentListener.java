package org.atlasapi.persistence.content;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.PreDestroy;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.Item;

import com.google.common.collect.Lists;

public class QueueingContentListener implements ContentListener {
    
    private static final Log log = LogFactory.getLog(QueueingContentListener.class);

    private final ContentListener delegate;

    private final BlockingQueue<Brand> brandQueue = new LinkedBlockingQueue<Brand>();
    private final BlockingQueue<Item> itemQueue = new LinkedBlockingQueue<Item>();

    private final ScheduledExecutorService executor;

    public QueueingContentListener(ContentListener delegate) {
        this(Executors.newScheduledThreadPool(4), delegate);
    }

    public QueueingContentListener(ScheduledExecutorService executor, ContentListener delegate) {
        this.executor = executor;
        this.delegate = delegate;
    }

    public void start() {
        executor.scheduleWithFixedDelay(new BrandChangedJob(), 60, 120, TimeUnit.SECONDS);
        executor.scheduleWithFixedDelay(new ItemChangedJob(), 30, 120, TimeUnit.SECONDS);
    }

    @Override
    public void brandChanged(Collection<Brand> brands, changeType changeType) {
        if (changeType == ContentListener.changeType.BOOTSTRAP) {
            delegate.brandChanged(brands, changeType);
        } else {
            brandQueue.addAll(brands);
        }
    }

    @Override
    public void itemChanged(Collection<Item> items, changeType changeType) {
        if (changeType == ContentListener.changeType.BOOTSTRAP) {
            delegate.itemChanged(items, changeType);
        } else {
            itemQueue.addAll(items);
        }
    }

    class BrandChangedJob implements Runnable {
        @Override
        public void run() {
            try {
                List<Brand> brands = Lists.newArrayList();
                brandQueue.drainTo(brands);

                delegate.brandChanged(brands, null);
            } catch (Exception e) {
                log.error("Delgate content listener failed to process brand queue", e);
            }
        }
    }

    class ItemChangedJob implements Runnable {
        @Override
        public void run() {
            try {
                List<Item> items = Lists.newArrayList();
                itemQueue.drainTo(items);

                delegate.itemChanged(items, null);
            } catch (Exception e) {
                log.error("Delgate content listener failed to process item queue", e);
            }
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
    }
}
