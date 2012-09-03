package org.atlasapi.persistence.bootstrap.elasticsearch;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.bootstrap.ContentChangeListener;
import org.atlasapi.persistence.content.elasticsearch.ESContentIndexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class ESChangeListener implements ContentChangeListener {

    private final Logger log = LoggerFactory.getLogger(ESChangeListener.class);
    //
    private final ESContentIndexer contentIndexer;
    private final ThreadPoolExecutor executor;

    public ESChangeListener(ESContentIndexer contentIndexer, int concurrencyLevel) {
        this(contentIndexer,
                new ThreadPoolExecutor(concurrencyLevel,
                concurrencyLevel,
                0,
                TimeUnit.MICROSECONDS,
                new ArrayBlockingQueue<Runnable>(25),
                new ThreadFactoryBuilder().setNameFormat(ESChangeListener.class + " Thread %d").build(), new ThreadPoolExecutor.CallerRunsPolicy()));
    }

    public ESChangeListener(ESContentIndexer contentIndexer, ThreadPoolExecutor executor) {
        this.contentIndexer = contentIndexer;
        this.executor = executor;
    }

    @Override
    public void beforeContentChange() {
        // No-op
    }

    @Override
    public void afterContentChange() {
        // No-op
    }

    @Override
    public void contentChange(Iterable<? extends Described> contents) {
        for (final Described content : contents) {
            if (content instanceof Item) {
                executor.submit(new Runnable() {

                    @Override
                    public void run() {
                        try {
                            contentIndexer.index((Item) content);
                        } catch (Exception ex) {
                            log.warn("Failed to index content {}, exception follows.", content);
                            log.warn(ex.getMessage(), ex);
                        }
                    }
                });
            } else {
                log.info("Cannot index content of type: {}", content.getClass());
            }
        }
    }
}
