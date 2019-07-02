package org.atlasapi.persistence.content.listing;

import java.util.Iterator;

import org.atlasapi.media.entity.Content;

public interface SelectedContentLister {

    Iterator<String> listContentUris(ContentListingCriteria criteria);

    Iterator<Content> listContent(ContentListingCriteria criteria);

    Iterator<Content> listUnpublishedContent(ContentListingCriteria criteria);
}
