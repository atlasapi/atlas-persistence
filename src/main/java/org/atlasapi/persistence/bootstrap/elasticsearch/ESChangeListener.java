package org.atlasapi.persistence.bootstrap.elasticsearch;

import java.util.concurrent.ThreadPoolExecutor;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.bootstrap.AbstractMultiThreadedChangeListener;
import org.atlasapi.persistence.content.IndexException;
import org.atlasapi.persistence.content.elasticsearch.EsContentIndexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class ESChangeListener extends AbstractMultiThreadedChangeListener {

    private final Logger log = LoggerFactory.getLogger(ESChangeListener.class);
    
    private EsContentIndexer esContentIndexer;

    public ESChangeListener(int concurrencyLevel) {
        super(concurrencyLevel);
    }

    public ESChangeListener(ThreadPoolExecutor executor) {
        super(executor);
    }

    public void setESContentIndexer(EsContentIndexer esContentIndexer) {
        this.esContentIndexer = esContentIndexer;
    }

    @Override
    protected void onChange(Object change) {
        if (change instanceof Item) {
            try {
                esContentIndexer.index((Item) change);
            } catch (IndexException e) {
                log.error("Failed to index " + change, e);
            }
        } else if (change instanceof Container) {
            esContentIndexer.index((Container) change);
        } else {
            throw new IllegalStateException("Unknown type: " + change.getClass().getName());
        }
    }
}
