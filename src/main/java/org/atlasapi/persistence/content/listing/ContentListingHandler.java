package org.atlasapi.persistence.content.listing;

import org.atlasapi.media.entity.Content;

public interface ContentListingHandler {
    
    boolean handle(Iterable<? extends Content> contents, ContentListingProgress progress);
    
}