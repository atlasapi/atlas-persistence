package org.atlasapi.persistence.bootstrap.elasticsearch;

import java.util.concurrent.ThreadPoolExecutor;

import org.atlasapi.media.content.Container;
import org.atlasapi.media.content.ContentIndexer;
import org.atlasapi.media.content.IndexException;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.bootstrap.AbstractMultiThreadedChangeListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexingChangeListener extends AbstractMultiThreadedChangeListener {

    private final Logger log = LoggerFactory.getLogger(IndexingChangeListener.class);
    
    private ContentIndexer esContentIndexer;

    public IndexingChangeListener(int concurrencyLevel) {
        super(concurrencyLevel);
    }

    public IndexingChangeListener(ThreadPoolExecutor executor) {
        super(executor);
    }

    public void setESContentIndexer(ContentIndexer esContentIndexer) {
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
