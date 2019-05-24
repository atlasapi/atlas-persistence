package org.atlasapi.persistence.content.listing;

import java.util.Iterator;

import org.atlasapi.media.entity.Content;

import com.metabroadcast.common.persistence.mongo.MongoSelectBuilder;

public interface SelectedContentLister {

    Iterator<Content> listContent(ContentListingCriteria criteria, boolean fetchSelected);

}
