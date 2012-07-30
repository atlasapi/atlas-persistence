package org.atlasapi.persistence;

import java.io.IOException;

import org.atlasapi.persistence.content.elasticsearch.ESContentIndexer;
import org.atlasapi.persistence.content.elasticsearch.EsScheduleIndex;

public class ElasticSearchContentIndexModule {

    private final ESContentIndexer contentIndexer;
    private EsScheduleIndex scheduleIndex;

    public ElasticSearchContentIndexModule(String seeds, long requestTimeout) {
        this.contentIndexer = new ESContentIndexer(seeds, requestTimeout);
        this.scheduleIndex = new EsScheduleIndex(seeds);
    }

    public void init() {
        try {
            contentIndexer.init();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public ESContentIndexer contentIndexer() {
        return contentIndexer;
    }

    public EsScheduleIndex scheduleIndex() {
        return scheduleIndex;
    }
}
