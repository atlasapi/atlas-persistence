package org.atlasapi.persistence.content.listing;

import static com.google.common.base.Objects.toStringHelper;

import java.util.List;

import org.atlasapi.media.entity.Publisher;
import org.atlasapi.persistence.content.ContentCategory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class ContentListingCriteria {

    public static class Builder {
        
        private ContentListingProgress progress = ContentListingProgress.START;
        private List<Publisher> publishers = Lists.newArrayList();
        private List<ContentCategory> categories = Lists.newArrayList();
        
        public Builder forPublishers(List<Publisher> publishers) {
            this.publishers.addAll(publishers);
            return this;
        }
        
        public Builder forPublishers(Publisher... publishers) {
            this.publishers.addAll(ImmutableList.copyOf(publishers));
            return this;
        }
        
        public Builder forPublisher(Publisher publisher) {
            this.publishers.add(publisher);
            return this;
        }
        
        public Builder startingAt(ContentListingProgress progress) {
            this.progress = progress;
            return this;
        }
        
        public Builder forContent(List<ContentCategory> categories) {
            this.categories.addAll(categories);
            return this;
        }
        
        public Builder forContent(ContentCategory... categories) {
            this.categories.addAll(ImmutableList.copyOf(categories));
            return this;
        }
        
        public Builder forContent(ContentCategory table) {
            this.categories.add(table);
            return this;
        }
        
        public ContentListingCriteria build() {
            return new ContentListingCriteria(categories, publishers, progress);
        }
        
    }

    public static final Builder defaultCriteria() { 
        return new Builder();
    }
    

    private final List<ContentCategory> tables;
    private final List<Publisher> publishers;
    private final ContentListingProgress progress;

    private ContentListingCriteria(List<ContentCategory> tables, List<Publisher> publishers, ContentListingProgress progress) {
        this.tables = tables;
        this.publishers = publishers;
        this.progress = progress;
    }
    
    public ContentListingProgress getProgress() {
        return this.progress;
    }
    
    public List<Publisher> getPublishers() {
        return this.publishers;
    }


    public List<ContentCategory> getCategories() {
        return tables;
    }
    
    @Override
    public String toString() {
        return toStringHelper(this).add("tables", tables).add("publishers", publishers).addValue(progress).toString();
    }
}
