package org.atlasapi.persistence.content;

import java.util.Set;

public interface ContentLister {

    /**
     * 
     * @param tables
     * @param progress
     * @param handler
     * @return true if the listing finished, false if interrupted by handler
     */
    boolean listContent(Set<ContentTable> tables, ContentListingProgress progress, ContentListingHandler handler);
    
}
