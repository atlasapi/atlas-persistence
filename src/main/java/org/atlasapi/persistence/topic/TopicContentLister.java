package org.atlasapi.persistence.topic;

import java.util.Iterator;

import org.atlasapi.content.criteria.ContentQuery;
import org.atlasapi.media.common.Id;
import org.atlasapi.media.content.Content;

public interface TopicContentLister {

    Iterator<Content> contentForTopic(Id topicId, ContentQuery contentQuery); 
    
}
