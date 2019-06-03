package org.atlasapi.persistence.content.listing;

import java.util.Iterator;
import java.util.List;

import org.atlasapi.media.entity.Content;

public interface SelectedContentLister {

    List<String> listContentUris(ContentListingCriteria criteria);

    Iterator<Content> listContent(ContentListingCriteria criteria);

}
