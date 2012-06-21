package org.atlasapi.persistence;

import org.atlasapi.persistence.content.ContentIndexer;
import org.atlasapi.persistence.content.elasticsearch.ESContentIndexer;

public class ElasticSearchContentIndexModule implements ContentIndexModule{
    
    private final String seeds;
    private final long requestTimeout;
    private final ContentIndexer contentIndexer;

    public ElasticSearchContentIndexModule(String seeds, long requestTimeout) {
        this.seeds = seeds;
        this.requestTimeout = requestTimeout;
        this.contentIndexer = new ESContentIndexer(seeds, requestTimeout);
    }

    public ContentIndexer contentIndexer() {
        return contentIndexer;
    }
}
