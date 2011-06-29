package org.atlasapi.persistence.content.listing;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;

import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableSet;

public class ContentListingCriteria {

    public static final ContentListingCriteria defaultCriteria() { 
        return new ContentListingCriteria();
    }
    
    private ContentListingProgress progress = ContentListingProgress.START;
    
    private Set<Publisher> publishers = null;

    private DateTime updatedSince;
    
    public ContentListingCriteria startingAt(ContentListingProgress progress) {
        this.progress = checkNotNull(progress);
        return this;
    }
    
    public ContentListingCriteria forPublisher(Publisher publisher) {
        this.publishers = ImmutableSet.of(publisher);
        return this;
    }
    
    public ContentListingCriteria forPublishers(Iterable<Publisher> publishers) {
        this.publishers = ImmutableSet.copyOf(publishers);
        return this;
    }
    
    public ContentListingProgress getProgress() {
        return this.progress;
    }
    
    public Set<Publisher> getPublishers() {
        return this.publishers;
    }
    
    public DateTime getUpdatedSince() {
        return updatedSince;
    }

    public ContentListingCriteria updatedSince(DateTime from) {
        this.updatedSince = from;
        return null;
    }
}
