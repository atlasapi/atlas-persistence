package org.atlasapi.persistence.content;

import org.atlasapi.media.entity.Identified;

public class ContentListingProgress {
    
    public static final ContentListingProgress START = new ContentListingProgress(null, null);
    
    public static final ContentListingProgress valueOf(Identified ided, ContentTable table) {
        return new ContentListingProgress(ided.getCanonicalUri(), table);
    }
    
    private final String uri;
    private final ContentTable table;

    public ContentListingProgress(String uri, ContentTable table) {
        this.uri = uri;
        this.table = table;
    }

    public String getUri() {
        return uri;
    }

    public ContentTable getTable() {
        return table;
    }
    
    
}
