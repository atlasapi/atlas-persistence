package org.atlasapi.persistence.content.listing;

import java.util.Iterator;
import java.util.List;

import org.atlasapi.media.entity.Content;

public interface SelectedContentLister {

    List<String> listContent(ContentListingCriteria criteria, boolean preloadAllContent);

    Iterator<Content> listContent(ContentListingCriteria criteria);

}
