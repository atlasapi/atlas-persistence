package org.atlasapi.persistence.content.listing;

import org.atlasapi.media.entity.Identified;
import org.atlasapi.persistence.content.ContentCategory;

public class ContentListingProgress {
    
    public static final ContentListingProgress START = new ContentListingProgress(null, null);
    
    public static final ContentListingProgress progressFor(Identified ided, ContentCategory table) {
        return new ContentListingProgress(ided.getCanonicalUri(), table);
    }
    
    private final String uri;
    private final ContentCategory table;

    private int total = 0;
    private int count = 0;

    public ContentListingProgress(String uri, ContentCategory table) {
        this.uri = uri;
        this.table = table;
    }

    public String getUri() {
        return uri;
    }

    public ContentCategory getTable() {
        return table;
    }
    
    public ContentListingProgress withCount(int count) {
        this.count = count;
        return this;
    }
    
    public ContentListingProgress withTotal(int total) {
        this.total = total;
        return this;
    }
    
    public int count() {
        return this.count;
    }
    
    public int total() {
        return this.total;
    }
    
    @Override
    public String toString() {
        return String.format("%s:%s (%s/%s)", table, uri, count, total);
    }
}
