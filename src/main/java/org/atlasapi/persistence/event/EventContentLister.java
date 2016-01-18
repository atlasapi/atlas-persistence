package org.atlasapi.persistence.event;

import java.util.Iterator;
import java.util.List;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.entity.Content;

public interface EventContentLister {

    Iterator<Content> contentForEvent(List<Long> eventIds, ContentQuery query);

}
