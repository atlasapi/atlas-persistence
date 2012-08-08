package org.atlasapi.persistence;

import java.io.IOException;
import org.atlasapi.persistence.content.ContentIndexer;
import org.atlasapi.persistence.content.elasticsearch.ESContentIndexer;

public class ElasticSearchContentIndexModule implements ContentIndexModule {

    private final ESContentIndexer contentIndexer;

    public ElasticSearchContentIndexModule(String seeds, long requestTimeout) {
        this.contentIndexer = new ESContentIndexer(seeds, requestTimeout);
    }

    @Override
    public void init() {
        try {
            contentIndexer.init();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public ContentIndexer contentIndexer() {
        return contentIndexer;
    }
}
