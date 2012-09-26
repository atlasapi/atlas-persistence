package org.atlasapi.persistence.bootstrap;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.entity.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public abstract class AbstractMultiThreadedChangeListener implements ChangeListener {

    private final Logger log = LoggerFactory.getLogger(AbstractMultiThreadedChangeListener.class);
    //
    private final ThreadPoolExecutor executor;

    public AbstractMultiThreadedChangeListener(int concurrencyLevel) {
        this(new ThreadPoolExecutor(concurrencyLevel,
                concurrencyLevel,
                0,
                TimeUnit.MICROSECONDS,
                new ArrayBlockingQueue<Runnable>(100 * Runtime.getRuntime().availableProcessors()),
                new ThreadFactoryBuilder().setNameFormat(AbstractMultiThreadedChangeListener.class + " Thread %d").build(), new ThreadPoolExecutor.CallerRunsPolicy()));
    }

    public AbstractMultiThreadedChangeListener(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void beforeChange() {
        // No-op
    }

    @Override
    public void afterChange() {
        // No-op
    }

    @Override
    public void onChange(Iterable changed) {
        for (final Object change : changed) {
            executor.submit(new Runnable() {

                @Override
                public void run() {
                    try {
                        onChange((Item) change);
                    } catch (Exception ex) {
                        log.warn("Failed to index content {}, exception follows.", change);
                        log.warn(ex.getMessage(), ex);
                    }
                }
            });
        }
    }

    protected abstract void onChange(Object change);
}
