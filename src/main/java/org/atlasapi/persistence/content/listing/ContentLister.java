package org.atlasapi.persistence.content.listing;

import java.util.Set;

import org.atlasapi.persistence.content.ContentCategory;

public interface ContentLister {

    /**
     * 
     * @param tables
     * @param criteria
     * @param handler
     * @return true if the listing finished, false if interrupted by handler
     */
    boolean listContent(Set<ContentCategory> tables, ContentListingCriteria criteria, ContentListingHandler handler);
    
}
