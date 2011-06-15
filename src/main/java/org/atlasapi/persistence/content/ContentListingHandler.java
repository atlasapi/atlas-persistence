package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Content;

public interface ContentListingHandler {
    
    boolean handle(Content content, ContentListingProgress progress);
    
}