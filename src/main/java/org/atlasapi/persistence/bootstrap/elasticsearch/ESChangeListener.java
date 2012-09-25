package org.atlasapi.persistence.bootstrap.elasticsearch;

import java.util.concurrent.ThreadPoolExecutor;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Item;
import org.atlasapi.persistence.bootstrap.AbstractMultiThreadedChangeListener;
import org.atlasapi.persistence.content.elasticsearch.ESContentIndexer;

/**
 */
public class ESChangeListener extends AbstractMultiThreadedChangeListener {

    private ESContentIndexer esContentIndexer;

    public ESChangeListener(int concurrencyLevel) {
        super(concurrencyLevel);
    }

    public ESChangeListener(ThreadPoolExecutor executor) {
        super(executor);
    }

    public void setESContentIndexer(ESContentIndexer esContentIndexer) {
        this.esContentIndexer = esContentIndexer;
    }

    @Override
    protected void onChange(Identified change) {
        esContentIndexer.index((Item) change);
    }
}
