package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Content;

public interface ContentListingHandler {
    
    void handle(Content content, ContentListingProgress progress);
    
}