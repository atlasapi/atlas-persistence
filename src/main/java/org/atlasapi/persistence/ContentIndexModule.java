package org.atlasapi.persistence;

import org.atlasapi.persistence.content.ContentIndexer;

/**
 */
public interface ContentIndexModule {
    
    public void init();
    
    public ContentIndexer contentIndexer();
}
