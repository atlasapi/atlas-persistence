package org.atlasapi.persistence.content;

import java.util.Set;

public interface ContentLister {

    void listContent(Set<ContentTable> tables, ContentListingProgress progress, ContentListingHandler handler);
    
}
