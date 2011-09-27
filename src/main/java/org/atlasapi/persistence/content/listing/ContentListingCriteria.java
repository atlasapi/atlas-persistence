package org.atlasapi.persistence.content.listing;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;

public class ContentListingCriteria {

    public static final ContentListingCriteria defaultCriteria() { 
        return new ContentListingCriteria();
    }
    
    private ContentListingProgress progress = ContentListingProgress.START;
    
    private List<Publisher> publishers = null;
    
    public ContentListingCriteria startingAt(ContentListingProgress progress) {
        this.progress = checkNotNull(progress);
        return this;
    }
    
    public ContentListingCriteria forPublisher(Publisher publisher) {
        this.publishers = ImmutableList.of(publisher);
        return this;
    }
    
    public ContentListingCriteria forPublishers(List<Publisher> publishers) {
        this.publishers = ImmutableList.copyOf(publishers);
        return this;
    }
    
    public ContentListingProgress getProgress() {
        return this.progress;
    }
    
    public List<Publisher> getPublishers() {
        return this.publishers;
    }
}
