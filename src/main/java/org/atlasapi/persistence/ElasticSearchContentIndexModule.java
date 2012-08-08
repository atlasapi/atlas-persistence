package org.atlasapi.persistence;

import java.io.IOException;
import org.atlasapi.persistence.content.elasticsearch.ESContentIndexer;

public class ElasticSearchContentIndexModule {

    private final ESContentIndexer contentIndexer;

    public ElasticSearchContentIndexModule(String seeds, long requestTimeout) {
        this.contentIndexer = new ESContentIndexer(seeds, requestTimeout);
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
}
