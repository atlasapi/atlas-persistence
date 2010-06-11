package org.uriplay.persistence.content;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.InitializingBean;
import org.uriplay.media.entity.Brand;
import org.uriplay.media.entity.Item;

import com.google.common.collect.Lists;

public class QueueingContentListener implements ContentListener, InitializingBean {

    private final ContentListener delegate;

    private final BlockingQueue<Brand> brandQueue = new LinkedBlockingQueue<Brand>();
    private final BlockingQueue<Item> itemQueue = new LinkedBlockingQueue<Item>();

    private final ScheduledExecutorService executor;

    public QueueingContentListener(ContentListener delegate) {
        this(Executors.newScheduledThreadPool(10), delegate);
    }

    public QueueingContentListener(ScheduledExecutorService executor, ContentListener delegate) {
        this.executor = executor;
        this.delegate = delegate;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        executor.scheduleAtFixedRate(new BrandChangedJob(), 60, 120, TimeUnit.SECONDS);
        executor.scheduleAtFixedRate(new ItemChangedJob(), 30, 120, TimeUnit.SECONDS);
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
            List<Brand> brands = Lists.newArrayList();
            brandQueue.drainTo(brands);

            delegate.brandChanged(brands, null);
        }
    }

    class ItemChangedJob implements Runnable {
        @Override
        public void run() {
            List<Item> items = Lists.newArrayList();
            itemQueue.drainTo(items);

            delegate.itemChanged(items, null);
        }
    }

    public void shutdown() {
        executor.shutdown();
    }
}
